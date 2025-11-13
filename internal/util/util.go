package util

import (
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
)

func JoinHostPort(addr string, port int) string {
	return net.JoinHostPort(addr, strconv.Itoa(port))
}

// GetEnvVariable returns environment variable by name
func GetEnvVariable(name, def string) string {
	if val, ok := os.LookupEnv(name); ok {
		return val
	}
	return def
}

// SelectNode returns host (from given list) starting specified match string
// If match starts with ^ it's discarded (backward compatibility)
func SelectNodes(hosts []string, match string) []string {
	match = strings.TrimPrefix(match, "^")
	res := make([]string, 0)
	for _, host := range hosts {
		if strings.HasPrefix(host, match) {
			res = append(res, host)
		}
	}
	return res
}

func TouchFile(fname string) error {
	_, err := os.Stat(fname)
	if os.IsNotExist(err) {
		err := os.WriteFile(fname, []byte(""), 0o644)
		if err != nil {
			return err
		}
	}
	return nil
}

func RunParallel(f func(string) error, arguments []string) map[string]error {
	type pair struct {
		key string
		err error
	}
	errs := make(chan pair, len(arguments))
	for _, argValue := range arguments {
		go func(dbname string) {
			errs <- pair{dbname, f(dbname)}
		}(argValue)
	}
	result := make(map[string]error)
	for range len(arguments) {
		pairValue := <-errs
		result[pairValue.key] = pairValue.err
	}
	return result
}

func CombineErrors(allErrors map[string]error) error {
	var errStr string
	for _, err := range allErrors {
		if err != nil {
			errStr += err.Error() + ";"
		}
	}
	if errStr != "" {
		return errors.New(errStr)
	}
	return nil
}

func FilterStrings(heap []string, cond func(s string) bool) []string {
	var ret []string
	for _, v := range heap {
		if cond(v) {
			ret = append(ret, v)
		}
	}
	return ret
}

func Union[T any](slices ...[]T) []T {
	res := make([]T, 0)
	for _, s := range slices {
		res = append(res, s...)
	}
	return res
}

func Ptr[T any](v T) *T {
	return &v
}

func JoinErrors(errors []error, sep string) string {
	strs := make([]string, 0, len(errors))
	for _, err := range errors {
		strs = append(strs, err.Error())
	}
	return strings.Join(strs, sep)
}
