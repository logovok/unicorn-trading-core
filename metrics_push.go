package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

const (
	GRAFANA_REMOTE_WRITE_URL = "https://prometheus-prod-65-prod-eu-west-2.grafana.net/api/prom/push"
	GRAFANA_USER             = "2907810"
	GRAFANA_PASSWORD         = "glc_eyJvIjoiMTYzODEwNiIsIm4iOiJzdGFjay0xNDkxNzg5LWFsbG95LXRlc3QiLCJrIjoiM2k1OUFROGoxbjE1Qk9zYjhDT0hnNjVqIiwibSI6eyJyIjoicHJvZC1ldS13ZXN0LTIifX0="
)

type RemoteWriter struct {
	client *http.Client
}

func NewRemoteWriter() *RemoteWriter {
	return &RemoteWriter{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (rw *RemoteWriter) Push(
	metric string,
	value float64,
	labels map[string]string,
) error {

	lbls := []prompb.Label{
		{Name: "__name__", Value: metric},
	}

	for k, v := range labels {
		lbls = append(lbls, prompb.Label{
			Name:  k,
			Value: v,
		})
	}

	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: lbls,
				Samples: []prompb.Sample{
					{
						Value:     value,
						Timestamp: time.Now().UnixMilli(),
					},
				},
			},
		},
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)

	httpReq, err := http.NewRequest(
		http.MethodPost,
		GRAFANA_REMOTE_WRITE_URL,
		bytes.NewReader(compressed),
	)
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq.SetBasicAuth(GRAFANA_USER, GRAFANA_PASSWORD)

	resp, err := rw.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 300 {
		return fmt.Errorf(
			"remote write failed: %s, body: %s",
			resp.Status,
			string(body),
		)
	}

	//log.Println("metrics pushed OK:", string(body))

	return nil
}
