package agreement

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io"
	"time"
)

// One representation is shared by promises, bulk manifests and Bridge payloads.
func (c *coldReplica) encodeRecovery(v any) ([]byte, error) {
	start := time.Now()
	var rawBuffer bytes.Buffer
	err := gob.NewEncoder(&rawBuffer).Encode(v)
	raw := rawBuffer.Bytes()
	if err != nil {
		return nil, err
	}
	if len(raw) > maxFrame {
		return nil, fmt.Errorf("recovery catalog exceeds frame bound")
	}
	var out bytes.Buffer
	writer, err := gzip.NewWriterLevel(&out, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err = writer.Write(raw); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	c.stats.RecoveryCodecInputBytes += uint64(len(raw))
	c.stats.RecoveryCodecOutputBytes += uint64(out.Len())
	c.stats.RecoveryCodecEncodeNanos += uint64(time.Since(start))
	return out.Bytes(), nil
}

func (c *coldReplica) decodeRecovery(data []byte, v any) error {
	start := time.Now()
	defer func() { c.stats.RecoveryCodecDecodeNanos += uint64(time.Since(start)) }()
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, maxFrame+1))
	if err != nil {
		return err
	}
	if len(raw) > maxFrame {
		return fmt.Errorf("recovery catalog exceeds frame bound")
	}
	return gob.NewDecoder(bytes.NewReader(raw)).Decode(v)
}
