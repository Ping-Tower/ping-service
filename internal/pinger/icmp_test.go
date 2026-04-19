package pinger

import (
	"errors"
	"testing"

	"pingtower/ping-service/internal/models"
)

func TestApplyICMPErrorMessage(t *testing.T) {
	t.Run("sets timeout text when ping failed without explicit error", func(t *testing.T) {
		result := models.PingRecordedPayload{IsSuccess: false}

		applyICMPErrorMessage(&result, nil)

		if result.ErrorMessage == nil {
			t.Fatal("expected error message to be set")
		}

		if got := *result.ErrorMessage; got != "icmp probe failed: no reply received" {
			t.Fatalf("unexpected error message: %s", got)
		}
	})

	t.Run("preserves explicit ping error", func(t *testing.T) {
		result := models.PingRecordedPayload{IsSuccess: false}

		applyICMPErrorMessage(&result, errors.New("context deadline exceeded"))

		if result.ErrorMessage == nil {
			t.Fatal("expected error message to be set")
		}

		if got := *result.ErrorMessage; got != "icmp probe failed: context deadline exceeded" {
			t.Fatalf("unexpected error message: %s", got)
		}
	})

	t.Run("keeps successful result empty", func(t *testing.T) {
		result := models.PingRecordedPayload{IsSuccess: true}

		applyICMPErrorMessage(&result, nil)

		if result.ErrorMessage != nil {
			t.Fatalf("expected no error message, got %s", *result.ErrorMessage)
		}
	})
}
