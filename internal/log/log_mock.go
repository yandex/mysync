package log

import (
	"log"
	"os"
)

func NewMockLogger() *MockLogger {
	return &MockLogger{
		log: log.New(os.Stdout, "", 0),
	}
}

type MockLogger struct {
	log *log.Logger
}

func (ml *MockLogger) Debug(msg string) {
	ml.log.Print(msg)
}

func (ml *MockLogger) Info(msg string) {
	ml.log.Print(msg)
}

func (ml *MockLogger) Warn(msg string) {
	ml.log.Print(msg)
}

func (ml *MockLogger) Error(msg string) {
	ml.log.Print(msg)
}

func (ml *MockLogger) Fatal(msg string) {
	ml.log.Print(msg)
}

func (ml *MockLogger) Debugf(msg string, args ...any) {
	ml.log.Printf(msg, args...)
}

func (ml *MockLogger) Infof(msg string, args ...any) {
	ml.log.Printf(msg, args...)
}

func (ml *MockLogger) Warnf(msg string, args ...any) {
	ml.log.Printf(msg, args...)
}

func (ml *MockLogger) Errorf(msg string, args ...any) {
	ml.log.Printf(msg, args...)
}

func (ml *MockLogger) Fatalf(msg string, args ...any) {
	ml.log.Printf(msg, args...)
}
