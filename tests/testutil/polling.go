package testutil

import "time"

// nolint: unparam
func Retry(code func() bool, timeout, sleep time.Duration) {
	if code() {
		return
	}
	timer := time.NewTimer(timeout)
	ticker := time.NewTicker(sleep)
	for {
		select {
		case <-ticker.C:
			if code() {
				return
			}
		case <-timer.C:
			return
		}
	}
}

// Eventually checks code until it succeeds or the timeout expires. Unlike
// Retry, it always performs a final check at the deadline.
func Eventually(code func() bool, timeout, sleep time.Duration) bool {
	if code() {
		return true
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(sleep)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if code() {
				return true
			}
		case <-timer.C:
			return code()
		}
	}
}

// Consistently checks that code succeeds for the whole duration, including a
// final check at the deadline.
func Consistently(code func() bool, duration, sleep time.Duration) bool {
	if !code() {
		return false
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()
	ticker := time.NewTicker(sleep)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !code() {
				return false
			}
		case <-timer.C:
			return code()
		}
	}
}
