// cmd/kvctl — KV store komut satırı istemcisi.
// Cluster'a bağlanıp KV komutlarını çalıştırmak için kullanılır.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/emin/kver/pkg/sdk"
	"github.com/spf13/cobra"
)

var (
	nodes  string
	client *sdk.Client
)

func main() {
	root := &cobra.Command{
		Use:   "kvctl",
		Short: "KVer distributed KV store CLI",
		Long:  `kvctl, kver cluster'ına bağlanarak KV operasyonları gerçekleştirir.`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if nodes == "" {
				return fmt.Errorf("--nodes is required")
			}
			nodeList := strings.Split(nodes, ",")
			client = sdk.NewClient(nodeList)
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if client != nil {
				client.Close()
			}
			return nil
		},
	}

	root.PersistentFlags().StringVar(&nodes, "nodes", "",
		"Comma-separated node addresses: localhost:7001,localhost:7002,localhost:7003")

	// ─── set ──────────────────────────────────────────────────────────────────
	setCmd := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a string value",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ttlSec, _ := cmd.Flags().GetInt("ttl")
			ttl := time.Duration(ttlSec) * time.Second
			return client.Set(args[0], args[1], ttl)
		},
	}
	setCmd.Flags().Int("ttl", 0, "TTL in seconds (0 = no expiry)")

	// ─── get ──────────────────────────────────────────────────────────────────
	getCmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Get a string value",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			val, err := client.Get(args[0])
			if err != nil {
				return err
			}
			fmt.Println(val)
			return nil
		},
	}

	// ─── del ──────────────────────────────────────────────────────────────────
	delCmd := &cobra.Command{
		Use:   "del <key>",
		Short: "Delete a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return client.Delete(args[0])
		},
	}

	// ─── incr ─────────────────────────────────────────────────────────────────
	incrCmd := &cobra.Command{
		Use:   "incr <key>",
		Short: "Increment a counter",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			val, err := client.Incr(args[0])
			if err != nil {
				return err
			}
			fmt.Println(val)
			return nil
		},
	}

	// ─── decr ─────────────────────────────────────────────────────────────────
	decrCmd := &cobra.Command{
		Use:   "decr <key>",
		Short: "Decrement a counter",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			val, err := client.Decr(args[0])
			if err != nil {
				return err
			}
			fmt.Println(val)
			return nil
		},
	}

	// ─── hset ─────────────────────────────────────────────────────────────────
	hsetCmd := &cobra.Command{
		Use:   "hset <key> <field> <value>",
		Short: "Set a hash field",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return client.HSet(args[0], args[1], args[2])
		},
	}

	// ─── hdel ─────────────────────────────────────────────────────────────────
	hdelCmd := &cobra.Command{
		Use:   "hdel <key> <field>",
		Short: "Delete a hash field",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return client.HDelete(args[0], args[1])
		},
	}

	// ─── hget ─────────────────────────────────────────────────────────────────
	hgetCmd := &cobra.Command{
		Use:   "hget <key> <field>",
		Short: "Get a hash field",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			val, err := client.HGet(args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Println(val)
			return nil
		},
	}

	// ─── hgetall ──────────────────────────────────────────────────────────────
	hgetallCmd := &cobra.Command{
		Use:   "hgetall <key>",
		Short: "Get all fields and values in a hash",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := client.HGetAll(args[0])
			if err != nil {
				return err
			}
			for k, v := range res {
				fmt.Printf("%s: %s\n", k, v)
			}
			return nil
		},
	}

	// ─── hexists ──────────────────────────────────────────────────────────────
	hexistsCmd := &cobra.Command{
		Use:   "hexists <key> <field>",
		Short: "Check if a hash field exists",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			exists, err := client.HExists(args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Println(exists)
			return nil
		},
	}

	// ─── lpush ────────────────────────────────────────────────────────────────
	lpushCmd := &cobra.Command{
		Use:   "lpush <key> <value...>",
		Short: "Push values to list head",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			n, err := client.LPush(args[0], args[1:]...)
			if err != nil {
				return err
			}
			fmt.Println(n)
			return nil
		},
	}

	// ─── rpush ────────────────────────────────────────────────────────────────
	rpushCmd := &cobra.Command{
		Use:   "rpush <key> <value...>",
		Short: "Push values to list tail",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			n, err := client.RPush(args[0], args[1:]...)
			if err != nil {
				return err
			}
			fmt.Println(n)
			return nil
		},
	}

	// ─── lpop ─────────────────────────────────────────────────────────────────
	lpopCmd := &cobra.Command{
		Use:   "lpop <key>",
		Short: "Pop value from list head",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			val, err := client.LPop(args[0])
			if err != nil {
				return err
			}
			fmt.Println(val)
			return nil
		},
	}

	// ─── rpop ─────────────────────────────────────────────────────────────────
	rpopCmd := &cobra.Command{
		Use:   "rpop <key>",
		Short: "Pop value from list tail",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			val, err := client.RPop(args[0])
			if err != nil {
				return err
			}
			fmt.Println(val)
			return nil
		},
	}

	// ─── lrange ───────────────────────────────────────────────────────────────
	lrangeCmd := &cobra.Command{
		Use:   "lrange <key> <start> <stop>",
		Short: "Get range of values from list",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			start, _ := strconv.Atoi(args[1])
			stop, _ := strconv.Atoi(args[2])
			vals, err := client.LRange(args[0], start, stop)
			if err != nil {
				return err
			}
			for _, v := range vals {
				fmt.Println(v)
			}
			return nil
		},
	}

	// ─── llen ─────────────────────────────────────────────────────────────────
	llenCmd := &cobra.Command{
		Use:   "llen <key>",
		Short: "Get list length",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			n, err := client.LLen(args[0])
			if err != nil {
				return err
			}
			fmt.Println(n)
			return nil
		},
	}

	// ─── zadd ─────────────────────────────────────────────────────────────────
	zaddCmd := &cobra.Command{
		Use:   "zadd <key> <score> <member>",
		Short: "Add to sorted set",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			score, err := strconv.ParseFloat(args[1], 64)
			if err != nil {
				return fmt.Errorf("invalid score: %v", err)
			}
			return client.ZAdd(args[0], score, args[2])
		},
	}

	// ─── zscore ───────────────────────────────────────────────────────────────
	zscoreCmd := &cobra.Command{
		Use:   "zscore <key> <member>",
		Short: "Get score of member in sorted set",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			score, err := client.ZScore(args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Println(score)
			return nil
		},
	}

	// ─── zrank ────────────────────────────────────────────────────────────────
	zrankCmd := &cobra.Command{
		Use:   "zrank <key> <member>",
		Short: "Get rank of member in sorted set",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			rank, err := client.ZRank(args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Println(rank)
			return nil
		},
	}

	// ─── zrange ───────────────────────────────────────────────────────────────
	zrangeCmd := &cobra.Command{
		Use:   "zrange <key> <start> <stop>",
		Short: "Get range of members from sorted set",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			start, _ := strconv.Atoi(args[1])
			stop, _ := strconv.Atoi(args[2])
			members, err := client.ZRange(args[0], start, stop)
			if err != nil {
				return err
			}
			for _, m := range members {
				fmt.Println(m)
			}
			return nil
		},
	}

	// ─── zrevrange ────────────────────────────────────────────────────────────
	zrevrangeCmd := &cobra.Command{
		Use:   "zrevrange <key> <start> <stop>",
		Short: "Get reverse range of members from sorted set",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			start, _ := strconv.Atoi(args[1])
			stop, _ := strconv.Atoi(args[2])
			members, err := client.ZRevRange(args[0], start, stop)
			if err != nil {
				return err
			}
			for _, m := range members {
				fmt.Println(m)
			}
			return nil
		},
	}

	// ─── zrem ─────────────────────────────────────────────────────────────────
	zremCmd := &cobra.Command{
		Use:   "zrem <key> <member>",
		Short: "Remove member from sorted set",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return client.ZRem(args[0], args[1])
		},
	}

	// ─── cluster ──────────────────────────────────────────────────────────────
	clusterCmd := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster management",
	}

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster status",
		RunE: func(cmd *cobra.Command, args []string) error {
			status, err := client.ClusterStatus()
			if err != nil {
				return err
			}
			fmt.Println(status)
			return nil
		},
	}

	addNodeCmd := &cobra.Command{
		Use:   "add-node <node-id> <address>",
		Short: "Add a node to cluster",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return client.AddNode(args[0], args[1])
		},
	}

	removeNodeCmd := &cobra.Command{
		Use:   "remove-node <node-id>",
		Short: "Remove a node from cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return client.RemoveNode(args[0])
		},
	}

	clusterCmd.AddCommand(statusCmd, addNodeCmd, removeNodeCmd)

	lrangeCmd.Flags().SetInterspersed(false)
	zrangeCmd.Flags().SetInterspersed(false)
	zrevrangeCmd.Flags().SetInterspersed(false)

	root.AddCommand(
		setCmd, getCmd, delCmd, incrCmd, decrCmd,
		hsetCmd, hdelCmd, hgetCmd, hgetallCmd, hexistsCmd,
		lpushCmd, rpushCmd, lpopCmd, rpopCmd, lrangeCmd, llenCmd,
		zaddCmd, zscoreCmd, zrankCmd, zrangeCmd, zrevrangeCmd, zremCmd,
		clusterCmd,
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
