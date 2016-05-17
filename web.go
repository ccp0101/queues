package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
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

const Timeout = 300

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

	redisPool := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisUrl.Host, redis.DialDatabase(db))

		if err != nil {
			return nil, err
		}

		return c, err
	}, 10)
	defer redisPool.Close()

	router := gin.Default()

	queueExists := func(r redis.Conn, qid string) bool {
		isMember, err := redis.Bool(r.Do("SISMEMBER", "queues", qid))
		if err != nil {
			panic(err)
		}
		return isMember
	}

	sanitizeQid := func(qid string) string {
		qid = strings.Replace(qid, "\n", "", -1)
		qid = strings.Replace(qid, "\r", "", -1)
		if len(qid) == 0 {
			panic("qid is empty")
		}
		return qid
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

	router.GET("/queues", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		queues, err := redis.Strings(r.Do("SMEMBERS", "queues"))
		if err != nil {
			panic(err)
		}

		c.String(http.StatusOK, strings.Join(queues, "\n"))
	})

	router.GET("/show/:qid", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))

		if queueExists(r, qid) {
			queued, err := redis.Int(r.Do("LLEN", "queues-"+qid+"-queued"))
			if err != nil {
				panic(err)
			}
			pending, err := redis.Int(r.Do("LLEN", "queues-"+qid+"-pending"))
			if err != nil {
				panic(err)
			}
			done, err := redis.Int(r.Do("LLEN", "queues-"+qid+"-done"))
			if err != nil {
				panic(err)
			}
			c.String(http.StatusOK, "Done: %d. Pending: %d. Queued: %d. All: %d. ",
				done, pending, queued, done+pending+queued)
		} else {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		}
	})

	router.GET("/show/:qid/queued", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))

		if queueExists(r, qid) {
			queued, err := redis.Strings(r.Do("LRANGE", "queues-"+qid+"-queued", 0, -1))
			if err != nil {
				panic(err)
			}
			c.String(http.StatusOK, strings.Join(queued, "\n"))
		} else {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		}
	})

	router.GET("/show/:qid/pending", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))

		if queueExists(r, qid) {
			pending, err := redis.Strings(r.Do("LRANGE", "queues-"+qid+"-pending", 0, -1))
			if err != nil {
				panic(err)
			}

			output := make([]string, 0, len(pending))
			ret := make(chan string, len(pending))
			var wg sync.WaitGroup
			for _, item := range pending {
				wg.Add(1)
				go func(item string) {
					r := redisPool.Get()
					defer r.Close()
					defer wg.Done()
					get, err := redis.String(r.Do("Get", "queues-"+qid+"-item-"+item+"-time"))
					if err != nil {
						panic(err)
					}
					ttl, err := redis.Int(r.Do("TTL", "queues-"+qid+"-item-"+item+"-time"))
					if err != nil {
						panic(err)
					}
					ret <- fmt.Sprintf("%s\t%s\t%d", item, get, ttl)
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
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))

		if queueExists(r, qid) {
			done, err := redis.Strings(r.Do("LRANGE", "queues-"+qid+"-done", 0, -1))
			if err != nil {
				panic(err)
			}
			c.String(http.StatusOK, strings.Join(done, "\n"))
		} else {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		}
	})

	router.POST("/new/:qid", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))

		if !queueExists(r, qid) {
			_, err := r.Do("SADD", "queues", qid)
			if err != nil {
				panic(err)
			}
			c.String(http.StatusOK, "Queue "+qid+" created.")
		} else {
			c.String(http.StatusBadRequest, "Queue "+qid+" already exists.")
		}
	})

	router.POST("/delete/:qid", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))

		if queueExists(r, qid) {
			r.Send("MULTI")
			r.Send("SREM", "queues", qid)
			r.Send("DEL", "queues-"+qid+"-queued", "queues-"+qid+"-pending",
				"queues-"+qid+"-done")
			_, err := r.Do("EXEC")
			if err != nil {
				panic(err)
			}
			c.String(http.StatusOK, "Queue "+qid+" deleted.")
		} else {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		}
	})

	router.POST("/enqueue/:qid", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))
		item := sanitizeItem(c.PostForm("item"))
		if !queueExists(r, qid) {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		} else {
			_, err := r.Do("RPush", "queues-"+qid+"-queued", item)
			if err != nil {
				panic(err)
			}
			c.String(http.StatusOK, "")
		}
	})

	router.POST("/next/:qid", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))

		if !queueExists(r, qid) {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		} else {
			item, err := redis.String(r.Do("RPOPLPUSH", "queues-"+qid+"-queued", "queues-"+qid+"-pending"))
			if item == "" {
				c.String(http.StatusOK, "")
			}

			ip, _, _ := net.SplitHostPort(c.Request.RemoteAddr)
			r.Send("MULTI")
			r.Send("SET", "queues-"+qid+"-item-"+item+"-time", ip)
			r.Send("EXPIRE", "queues-"+qid+"-item-"+item+"-time", Timeout)
			_, err = r.Do("EXEC")

			if err != nil {
				panic(err)
			}
			c.String(http.StatusOK, item)
		}
	})

	router.POST("/done/:qid", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))
		item := sanitizeItem(c.PostForm("item"))

		if !queueExists(r, qid) {
			c.String(http.StatusNotFound, "Queue "+qid+" does not exist.")
		} else {
			deleted, err := redis.Int(r.Do("LREM", "queues-"+qid+"-pending", 1, item))
			if err != nil {
				panic(err)
			}
			if deleted != 1 {
				c.String(http.StatusBadRequest, "%s was not in pending.", item)
			} else {
				_, err := r.Do("RPUSH", "queues-"+qid+"-done", item)
				if err != nil {
					panic(err)
				}

				c.String(http.StatusOK, item)
			}
		}
	})

	router.POST("/extend/:qid", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))
		item := sanitizeItem(c.PostForm("item"))

		exp, err := redis.Bool(r.Do("EXPIRE", "queues-"+qid+"-item-"+item+"-time", Timeout))
		if err != nil {
			panic(err)
		}
		if !exp {
			c.String(http.StatusBadRequest, "%v was not found.", item)
		}
		c.String(http.StatusOK, item)
	})

	router.POST("/ttl/:qid", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))
		item := sanitizeItem(c.PostForm("item"))

		ttl, err := redis.Int(r.Do("TTL", "queues-"+qid+"-item-"+item+"-time"))
		if err != nil {
			panic(err)
		}
		if ttl < 0 {
			c.String(http.StatusNotFound, "Item already expired?")
		} else {
			c.String(http.StatusOK, "%d", ttl)
		}
	})

	router.POST("/expire/:qid", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()
		qid := sanitizeQid(c.Param("qid"))
		item := sanitizeItem(c.PostForm("item"))

		_, err := r.Do("DEL", "queues-"+qid+"-item-"+item+"-time")
		if err != nil {
			panic(err)
		}
		c.String(http.StatusOK, item)
	})

	router.POST("/_clean", func(c *gin.Context) {
		r := redisPool.Get()
		defer r.Close()

		qids, err := redis.Strings(r.Do("SMEMBERS", "queues"))
		if err != nil {
			panic(err)
		}

		for _, qid := range qids {
			items, err := redis.Strings(r.Do("LRANGE", "queues-"+qid+"-pending", 0, -1))
			if err != nil {
				panic(err)
			}

			for _, item := range items {
				get, err := redis.String(r.Do("GET", "queues-"+qid+"-item-"+item+"-time"))
				if get == "" {
					_, err = r.Do("LREM", "queues-"+qid+"-pending", 1, item)
					if err != nil {
						panic(err)
					}

					_, err = r.Do("RPUSH", "queues-"+qid+"-queued", item)
					if err != nil {
						panic(err)
					}

					log.Printf("Put expired item %v back to queue %v", item, qid)
				}
			}
		}
		c.String(http.StatusOK, "")
	})

	monitorTimeout := func() {
		for {
			time.Sleep(5 * time.Second)
			http.PostForm("http://127.0.0.1:"+port+"/_clean", url.Values{})
		}
	}
	go monitorTimeout()

	panic(router.Run(":" + port))
}
