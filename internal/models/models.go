package models

import "time"

type Protocol string

const (
	ProtocolICMP  Protocol = "ICMP"
	ProtocolTCP   Protocol = "TCP"
	ProtocolHTTP  Protocol = "HTTP"
	ProtocolHTTPS Protocol = "HTTPS"
)

type ServerStatus string

const (
	ServerStatusUp      ServerStatus = "UP"
	ServerStatusDown    ServerStatus = "DOWN"
	ServerStatusUnknown ServerStatus = "UNKNOWN"
)

type ServerEventPayload struct {
	Server       ServerEventServer       `json:"server"`
	PingSettings ServerEventPingSettings `json:"pingSettings"`
}

type ServerEventServer struct {
	ID        string       `json:"id"`
	Name      string       `json:"name"`
	Host      string       `json:"host"`
	Query     *string      `json:"query,omitempty"`
	UserID    string       `json:"userId"`
	Port      *int         `json:"port,omitempty"`
	IsActive  bool         `json:"isActive"`
	Protocol  Protocol     `json:"protocol"`
	Status    ServerStatus `json:"status"`
	IsDeleted bool         `json:"isDeleted"`
}

type ServerEventPingSettings struct {
	ID                 string `json:"id"`
	ServerID           string `json:"serverId"`
	IntervalSec        *int   `json:"intervalSec,omitempty"`
	LatencyThresholdMs *int   `json:"latencyThresholdMs,omitempty"`
	Retries            *int   `json:"retries,omitempty"`
	FailureThreshold   *int   `json:"failureThreshold,omitempty"`
	IsDeleted          bool   `json:"isDeleted"`
}

type PingRecordedPayload struct {
	ID                string     `json:"id"`
	ServerID          string     `json:"serverId"`
	Protocol          Protocol   `json:"protocol"`
	Timestamp         time.Time  `json:"timestamp"`
	IsSuccess         bool       `json:"isSuccess"`
	LatencyMs         *float64   `json:"latencyMs,omitempty"`
	ErrorMessage      *string    `json:"errorMessage,omitempty"`
	StatusCode        *int       `json:"statusCode,omitempty"`
	CertExpiresAt     *time.Time `json:"certExpiresAt,omitempty"`
	TLSVersion        *string    `json:"tlsVersion,omitempty"`
	DNSLookupMs       *float64   `json:"dnsLookupMs,omitempty"`
	SentBytes         *int64     `json:"sentBytes,omitempty"`
	ReceivedBytes     *int64     `json:"receivedBytes,omitempty"`
	PacketLossPercent *float64   `json:"packetLossPercent,omitempty"`
	RTTMinMs          *float64   `json:"rttMinMs,omitempty"`
	RTTMaxMs          *float64   `json:"rttMaxMs,omitempty"`
	TTL               *int       `json:"ttl,omitempty"`
}