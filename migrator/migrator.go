package migrator

import (
	"context"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"redis-migrator/client"
	"redis-migrator/config"
	"redis-migrator/pkg/concurrency"
	"time"
)

// MigrateRedisData is the function to migrate keys from old to new redis
func MigrateRedisData(ctx context.Context, conf config.Configuration) error {
	logrus.Infof("Migrating from %s:%d to %s:%d", conf.OldRedis.Host, conf.OldRedis.Port, conf.NewRedis.Host, conf.NewRedis.Port)
	concurrentWorkers := max(1, conf.ConcurrentWorkers)
	if concurrentWorkers > uint(len(conf.Databases)) {
		concurrentWorkers = uint(len(conf.Databases))
	}
	logrus.Infof("Migrating with %d concurrent workers", concurrentWorkers)

	for _, database := range conf.Databases {
		oldRedisPool := client.NewPool(conf.OldRedis, database, int(conf.ConcurrentWorkers), time.Second*10)
		newRedisPool := client.NewPool(conf.NewRedis, database, int(conf.ConcurrentWorkers), time.Second*10)

		if conf.ClearBeforeMigration {
			if _, err := newRedisPool.Get().Do("FLUSHDB"); err != nil {
				return fmt.Errorf("[DB %d] Error while FLUSHDB", database)
			}
			logrus.Infof("[DB %d] Cleared DB before migration", database)
		}

		if err := migrateDB(ctx, oldRedisPool, newRedisPool, database, concurrentWorkers); err != nil {
			return errors.Join(err, fmt.Errorf("[DB %d] Error while migrating", database))
		}
	}
	return nil
}

func migrateDB(ctx context.Context, oldRedisPool, newRedisPool *redis.Pool, db int, concurrentWorkers uint) error {
	keys, err := redis.Strings(oldRedisPool.Get().Do("KEYS", "*"))
	if err != nil {
		return fmt.Errorf("[DB %d] Error while listing redis keys '%v'", db, err)
	}
	if len(keys) == 0 {
		return nil
	}

	workerPool := concurrency.NewWorkerPool(concurrentWorkers)
	logrus.Infof("[DB %d] Migrating %d keys", db, len(keys))
	for i := range keys {
		index := i
		workerPool.AddJob(func(ctx context.Context) error {
			if err := migrateKey(oldRedisPool.Get(), newRedisPool.Get(), keys[index]); err != nil {
				return errors.Join(err, fmt.Errorf("[DB %d] Error while migrating key %s", db, keys[index]))
			} else {
				logrus.Debugf("[DB %d] Migrated key #%d in %d keys", db, index, len(keys))
			}
			return nil
		})
	}
	jobErrs, err := workerPool.Run(ctx)
	if err != nil {
		return errors.Join(err, fmt.Errorf("[DB %d] Error while running worker pool", db))
	}
	var finalErr error
	for i, jobErr := range jobErrs {
		if jobErr != nil {
			finalErr = fmt.Errorf("[DB %d] Error while migrating keys", db)
			logrus.Errorf("[DB %d] Error while migrating keys: %s - Error: %s", db, keys[i], jobErr.Error())
		}
	}
	return finalErr
}

func migrateKey(oldClient redis.Conn, newClient redis.Conn, key string) error {
	logrus.Debugf("Migrating key '%s'", key)
	defer func() {
		err := newClient.Flush()
		if err != nil {
			logrus.Errorf("Error while flushing connection: %v", err)
		}
	}()
	keyType, err := redis.String(oldClient.Do("TYPE", key))
	if err != nil {
		return fmt.Errorf("failed to get the key type %s: %v", key, err)
	}
	switch keyType {
	case "string":
		return migrateString(oldClient, newClient, key)
	case "hash":
		return migrateHash(oldClient, newClient, key)
	case "list":
		return migrateList(oldClient, newClient, key)
	case "set":
		return migrateSet(oldClient, newClient, key)
	case "zset":
		return migrateSortedSet(oldClient, newClient, key)
	default:
		return errors.New(fmt.Sprintf("key type is not supported: %s", keyType))
	}
}

func migrateList(oldClient redis.Conn, newClient redis.Conn, key string) error {
	elements, err := redis.Strings(oldClient.Do("LRANGE", key, 0, -1))
	if err != nil {
		logrus.Errorf("Not able to get the elements for key %s: %v", key, err)
	}
	var data = []interface{}{key}
	for i := len(elements) - 1; i >= 0; i-- {
		data = append(data, elements[i])
	}
	// LPUSH: Elements are inserted one after the other to the head of the list, from the leftmost element to the rightmost element.
	// So for instance the command LPUSH mylist a b c will result into a list containing c as first element, b as second element and a as third element
	_, err = newClient.Do("LPUSH", data...)
	if err != nil {
		return fmt.Errorf("error while pushing list keys %v", err)
	}
	logrus.Tracef("Migrated %s key with elements: %v", key, elements)
	return nil
}

func migrateHash(oldClient redis.Conn, newClient redis.Conn, key string) error {
	value, err := redis.StringMap(oldClient.Do("HGETALL", key))
	if err != nil {
		logrus.Errorf("Not able to get the value for key %s: %v", key, err)
	}
	var data = []interface{}{key}
	for k, v := range value {
		data = append(data, k, v)
	}
	_, err = newClient.Do("HMSET", data...)
	if err != nil {
		return fmt.Errorf("HMSET error: %v", err)
	}
	logrus.Tracef("Migrated %s key with value: %v", key, data)
	return nil
}

func migrateString(oldClient redis.Conn, newClient redis.Conn, key string) error {
	value, err := redis.String(oldClient.Do("GET", key))
	if err != nil {
		logrus.Errorf("Not able to get the value for key %s: %v", key, err)
	}
	_, err = newClient.Do("SET", key, value)
	if err != nil {
		return fmt.Errorf("SET error: %v", err)
	}
	logrus.Tracef("Migrated %s key with value: %v", key, value)
	return nil
}

func migrateSet(oldClient redis.Conn, newClient redis.Conn, key string) error {
	members, err := redis.Strings(oldClient.Do("SMEMBERS", key))
	if err != nil {
		logrus.Errorf("SMEMBERS %s error: %s", key, err)
	}
	for i := 0; i < len(members); i++ {
		err := newClient.Send("SADD", key, members[i])
		if err != nil {
			return fmt.Errorf("SADD error: %v", err)
		}
	}

	logrus.Tracef("Migrated %s key with value: %v", key, members)
	return nil
}

func migrateSortedSet(oldClient redis.Conn, newClient redis.Conn, key string) error {
	members, err := redis.Strings(oldClient.Do("ZRANGE", key, "0", "-1", "WITHSCORES"))
	if err != nil {
		logrus.Errorf("ZRANGE %s error: %s", key, err)
	}
	// members will be like ["member1", "score1", "member2", "score2"]
	for i := 0; i < len(members); i += 2 {
		err := newClient.Send("ZADD", key, members[i+1], members[i])
		if err != nil {
			return fmt.Errorf("ZADD error: %v", err)
		}
	}

	logrus.Tracef("Migrated %s key with value: %v", key, members)
	return nil
}
