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
		if err := migrateDB(ctx, oldRedisPool, newRedisPool, database, concurrentWorkers); err != nil {
			return errors.Join(err, fmt.Errorf("[DB %d] Error while migrating", database))
		}
	}
	return nil
}

func migrateDB(ctx context.Context, oldRedisPool, newRedisPool *redis.Pool, db int, concurrentWorkers uint) error {
	keys, err := redis.Strings(oldRedisPool.Get().Do("KEYS", "*"))
	if err != nil {
		return fmt.Errorf("[DB %d] Error while listing redis keys %v", db, err)
	}
	if len(keys) == 0 {
		return nil
	}

	workerPool := concurrency.NewWorkerPool(concurrentWorkers)
	logrus.Infof("[DB %d] Migrating %d keys", db, len(keys))
	for i, key := range keys {
		index := i
		workerPool.AddJob(func(ctx context.Context) error {
			if err := migrateKey(oldRedisPool.Get(), newRedisPool.Get(), key); err != nil {
				return errors.Join(err, fmt.Errorf("[DB %d] Error while migrating key %s", db, key))
			}
			logrus.Debugf("[DB %d] Migrated key #%d in %d keys", db, index, len(keys))
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
	keyType, err := redis.String(oldClient.Do("TYPE", key))
	if err != nil {
		return fmt.Errorf("failed to get the key type %s: %v", key, err)
	}
	switch keyType {
	case "string":
		migrateStringKeys(oldClient, newClient, key)
	case "hash":
		migrateHashKeys(oldClient, newClient, key)
	case "list":
		migrateListKeys(oldClient, newClient, key)
	default:
		return errors.New(fmt.Sprintf("key type is not supported: %s", keyType))
	}
	//TODO: Support set, sorted set
	//TODO: Run concurrently
	//TODO: Support ignore error
	return nil
}

func migrateListKeys(oldClient redis.Conn, newClient redis.Conn, key string) {
	value, err := redis.Strings(oldClient.Do("LPOP", key))
	if err != nil {
		logrus.Errorf("Not able to get the value for key %s: %v", key, err)
	}
	var data = []interface{}{key}
	for _, v := range value {
		data = append(data, v)
	}
	_, err = newClient.Do("LPUSH", data...)
	if err != nil {
		logrus.Errorf("Error while pushing list keys %v", err)
	}
	logrus.Tracef("Migrated %s key with value: %v", key, data)
}

func migrateHashKeys(oldClient redis.Conn, newClient redis.Conn, key string) {
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
		logrus.Errorf("Error while pushing list keys %v", err)
	}
	logrus.Tracef("Migrated %s key with value: %v", key, data)
}

func migrateStringKeys(oldClient redis.Conn, newClient redis.Conn, key string) {
	value, err := redis.String(oldClient.Do("GET", key))
	if err != nil {
		logrus.Errorf("Not able to get the value for key %s: %v", key, err)
	}
	_, err = newClient.Do("SET", key, value)
	if err != nil {
		logrus.Errorf("Error while pushing list keys %v", err)
	}
	logrus.Tracef("Migrated %s key with value: %v", key, value)
}
