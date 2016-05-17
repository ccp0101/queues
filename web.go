package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"gopkg.in/redis.v3"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Timeout = 5 * time.Minute

func ParseRedistogoUrl() (string, string) {
	redisUrl := os.Getenv("REDIS_URL")
	redisInfo, _ := url.Parse(redisUrl)
	server := redisInfo.Host
	password := ""
	if redisInfo.User != nil {
		password, _ = redisInfo.User.Password()
	}
	return server, password
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "17901"
	}
	log.Printf("Port: %v", port)

	redisUrl, err := url.Parse(os.Getenv("REDIS_URL"))
	if err != nil {
		panic(err)
	}

	db, err := strconv.Atoi(strings.TrimLeft(redisUrl.Path, "/"))
	if err != nil {
		panic(err)
	}
	r := redis.NewClient(&redis.Options{
		Addr: redisUrl.Host,
		DB:   int64(db),
	})

	router := gin.Default()

	queueExists := func(qid string) bool {
		isMember := r.SIsMember("queues", qid)
		if isMember.Err() != nil {
			panic(isMember.Err())
		}
		return isMember.Val()
	}

	sanitizeItem := func(item string) string {
		item = strings.Replace(item, "\n", "", -1)
		item = strings.Replace(item, "\r", "", -1)
		if len(item) == 0 {
			panic("item is empty")
		}
		return item
	}

	router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "Sweet home!")
	})

	router.GET("/show/:qid", func(c *gin.Context) {
		qid := c.Param("qid")

		if queueExists(qid) {
			queued := r.LLen("queues-" + qid + "-queued")
			if queued.Err() != nil {
				panic(queued.Err())
			}
			pending := r.LLen("queues-" + qid + "-pending")
			if pending.Err() != nil {
				panic(pending.Err())
			}
			done := r.LLen("queues-" + qid + "-done")
			if pending.Err() != nil {
				panic(pending.Err())
			}
			c.String(http.StatusOK, "Done: %d. Pending: %d. Queued: %d. All: %d. ",
				done.Val(), pending.Val(), queued.Val(),
				done.Val()+pending.Val()+queued.Val())
		} else {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		}
	})

	router.GET("/show/:qid/queued", func(c *gin.Context) {
		qid := c.Param("qid")

		if queueExists(qid) {
			queued := r.LRange("queues-"+qid+"-queued", 0, -1)
			if queued.Err() != nil {
				panic(queued.Err())
			}
			c.String(http.StatusOK, strings.Join(queued.Val(), "\n"))
		} else {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		}
	})

	router.GET("/show/:qid/pending", func(c *gin.Context) {
		qid := c.Param("qid")

		if queueExists(qid) {
			pending := r.LRange("queues-"+qid+"-pending", 0, -1)
			if pending.Err() != nil {
				panic(pending.Err())
			}

			output := make([]string, 0, len(pending.Val()))
			ret := make(chan string, len(pending.Val()))
			var wg sync.WaitGroup
			for _, item := range pending.Val() {
				wg.Add(1)
				go func(item string) {
					defer wg.Done()
					get := r.Get("queues-" + qid + "-item-" + item + "-time")
					ttl := r.TTL("queues-" + qid + "-item-" + item + "-time")
					ret <- fmt.Sprintf("%s\t%s\t%d", item, get.Val(), ttl.Val()/time.Second)
				}(item)
			}
			wg.Wait()
			close(ret)
			for line := range ret {
				output = append(output, line)
			}
			c.String(http.StatusOK, strings.Join(output, "\n"))
		} else {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		}
	})

	router.GET("/show/:qid/done", func(c *gin.Context) {
		qid := c.Param("qid")

		if queueExists(qid) {
			done := r.LRange("queues-"+qid+"-done", 0, -1)
			if done.Err() != nil {
				panic(done.Err())
			}
			c.String(http.StatusOK, strings.Join(done.Val(), "\n"))
		} else {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		}
	})

	router.POST("/new/:qid", func(c *gin.Context) {
		qid := c.Param("qid")

		if !queueExists(qid) {
			add := r.SAdd("queues", qid)
			if add.Err() != nil {
				panic(add.Err())
			}
			c.String(http.StatusOK, "Queue "+qid+" created.")
		} else {
			c.String(http.StatusBadRequest, "Queue "+qid+" already exists.")
		}
	})

	router.POST("/delete/:qid", func(c *gin.Context) {
		qid := c.Param("qid")

		if queueExists(qid) {
			del := r.SRem("queues", qid)
			if del.Err() != nil {
				panic(del.Err())
			}
			del = r.Del("queues-"+qid+"-queued", "queues-"+qid+"-pending",
				"queues-"+qid+"-done")
			if del.Err() != nil {
				panic(del.Err())
			}
			c.String(http.StatusOK, "Queue "+qid+" deleted.")
		} else {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		}
	})

	router.POST("/enqueue/:qid", func(c *gin.Context) {
		qid := c.Param("qid")
		item := sanitizeItem(c.PostForm("item"))
		if !queueExists(qid) {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		} else {
			push := r.RPush("queues-"+qid+"-queued", item)
			if push.Err() != nil {
				panic(push.Err())
			}
			c.String(http.StatusOK, "")
		}
	})

	router.POST("/next/:qid", func(c *gin.Context) {
		qid := c.Param("qid")

		if !queueExists(qid) {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		} else {
			move := r.RPopLPush("queues-"+qid+"-queued", "queues-"+qid+"-pending")
			item := move.Val()
			if move == nil {
				c.String(http.StatusOK, "")
			}

			ip, _, _ := net.SplitHostPort(c.Request.RemoteAddr)
			mark := r.Set("queues-"+qid+"-item-"+item+"-time",
				ip, Timeout)
			if mark.Err() != nil {
				panic(mark.Err())
			}
			c.String(http.StatusOK, item)
		}
	})

	router.POST("/done/:qid", func(c *gin.Context) {
		qid := c.Param("qid")
		item := sanitizeItem(c.PostForm("item"))

		if !queueExists(qid) {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		} else {
			pending := r.LRem("queues-"+qid+"-pending", 1, item)
			if pending.Err() != nil {
				panic(pending.Err())
			}
			if pending.Val() != 1 {
				c.String(http.StatusBadRequest, "%s was not in pending.", item)
			} else {
				done := r.RPush("queues-"+qid+"-done", item)
				if done.Err() != nil {
					panic(done.Err())
				}

				c.String(http.StatusOK, item)
			}
		}
	})

	router.POST("/extend/:qid", func(c *gin.Context) {
		qid := c.Param("qid")
		item := sanitizeItem(c.PostForm("item"))

		mark := r.Expire("queues-"+qid+"-item-"+item+"-time", Timeout)
		if mark.Err() != nil {
			panic(mark.Err())
		}
		if !mark.Val() {
			c.String(http.StatusBadRequest, "%v was not found.", item)
		}
		c.String(http.StatusOK, item)
	})

	router.POST("/ttl/:qid", func(c *gin.Context) {
		qid := c.Param("qid")
		item := sanitizeItem(c.PostForm("item"))

		ttl := r.TTL("queues-" + qid + "-item-" + item + "-time")
		if ttl.Err() != nil {
			panic(ttl.Err())
		}
		if ttl.Val() < 0 {
			c.String(http.StatusNotFound, "Item already expired?")
		} else {
			c.String(http.StatusOK, "%d", ttl.Val()/time.Second)
		}
	})

	router.POST("/expire/:qid", func(c *gin.Context) {
		qid := c.Param("qid")
		item := sanitizeItem(c.PostForm("item"))

		del := r.Del("queues-" + qid + "-item-" + item + "-time")
		if del.Err() != nil {
			panic(del.Err())
		}
		c.String(http.StatusOK, item)
	})

	monitorTimeout := func() {
		for {
			time.Sleep(1 * time.Second)
			members := r.SMembers("queues")
			if members.Err() != nil {
				log.Printf("%v", members.Err())
				continue
			}
			qids := members.Val()
			for _, qid := range qids {
				pending := r.LRange("queues-"+qid+"-pending", 0, -1)
				if pending.Err() != nil {
					log.Printf("%v", pending.Err())
					continue
				}

				items := pending.Val()
				for _, item := range items {
					get := r.Get("queues-" + qid + "-item-" + item + "-time")
					if get.Val() == "" {
						rem := r.LRem("queues-"+qid+"-pending", 1, item)
						if rem.Err() != nil {
							log.Printf("%v", rem.Err())
							continue
						}

						push := r.RPush("queues-"+qid+"-queued", item)
						if push.Err() != nil {
							log.Printf("%v", push.Err())
							continue
						}

						log.Printf("Put expired item %v back to queue %v", item, qid)
					}
				}
			}
		}
	}
	go monitorTimeout()

	panic(router.Run(":" + port))
}
