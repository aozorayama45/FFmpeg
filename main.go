package main

import (
	"fmt"
	"sync"
	"time"
)

// Frame represents a simulated video frame
type Frame struct {
	ID   int
	Data []byte
}

// Packet represents a simulated encoded packet
type Packet struct {
	ID   int
	Data []byte
}

// Transcoder simulates a memory-safe transcoding pipeline
type Transcoder struct {
	frameQueue  chan *Frame
	packetQueue chan *Packet
	maxQueueSize int
	mu          sync.Mutex
	activeFrames int
	activePackets int
}

func NewTranscoder(maxQueueSize int) *Transcoder {
	return &Transcoder{
		frameQueue:   make(chan *Frame, maxQueueSize),
		packetQueue:  make(chan *Packet, maxQueueSize),
		maxQueueSize: maxQueueSize,
	}
}

func (t *Transcoder) AllocFrame(id int) *Frame {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.activeFrames++
	return &Frame{ID: id, Data: make([]byte, 1024*1024)} // 1MB simulated frame
}

func (t *Transcoder) FreeFrame(f *Frame) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if f != nil {
		f.Data = nil
		t.activeFrames--
	}
}

func (t *Transcoder) AllocPacket(id int) *Packet {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.activePackets++
	return &Packet{ID: id, Data: make([]byte, 100*1024)} // 100KB simulated packet
}

func (t *Transcoder) FreePacket(p *Packet) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p != nil {
		p.Data = nil
		t.activePackets--
	}
}

func (t *Transcoder) StartTranscoding(duration time.Duration) {
	stopChan := time.After(duration)
	var wg sync.WaitGroup

	// Decoder Goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(t.frameQueue)
		frameID := 0
		for {
			select {
			case <-stopChan:
				return
			default:
				frame := t.AllocFrame(frameID)
				select {
				case t.frameQueue <- frame:
					frameID++
				case <-stopChan:
					t.FreeFrame(frame)
					return
				}
			}
		}
	}()

	// Encoder Goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(t.packetQueue)
		for frame := range t.frameQueue {
			packet := t.AllocPacket(frame.ID)
			t.FreeFrame(frame) // Properly free frame after encoding

			select {
			case t.packetQueue <- packet:
			case <-stopChan:
				t.FreePacket(packet)
				return
			}
		}
	}()

	// Muxer Goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for packet := range t.packetQueue {
			// Simulate writing packet to output
			t.FreePacket(packet) // Properly free packet after muxing
		}
	}()

	wg.Wait()

	// Clean up any remaining items in queues
	for frame := range t.frameQueue {
		t.FreeFrame(frame)
	}
	for packet := range t.packetQueue {
		t.FreePacket(packet)
	}

	fmt.Printf("Transcoding finished. Active Frames: %d, Active Packets: %d\n", t.activeFrames, t.activePackets)
}

func main() {
	fmt.Println("Starting Memory-Safe Transcoder...")
	transcoder := NewTranscoder(10) // Limit queue size to prevent unbounded memory growth
	transcoder.StartTranscoding(2 * time.Second)
}
