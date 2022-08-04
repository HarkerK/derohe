package p2p

// this file implements CBOR codec to prevent from certain attacks
import "fmt"
import "bytes"
import "io"
import "net"
import "sync"
import "time"
import "github.com/cenkalti/rpc2"
import "encoding/binary"
import "github.com/fxamacker/cbor/v2"

import "github.com/deroproject/derohe/config" // only used get constants such as max data per frame

// reads our data, length prefix blocks
func Read_Data_Frame_KCP(r net.Conn, obj interface{}) error {
	var frame_length_buf [4]byte

	//connection.set_timeout()
	r.SetReadDeadline(time.Now().Add(READ_TIMEOUT))
	nbyte, err := io.ReadFull(r, frame_length_buf[:])
	if err != nil {
		return err
	}
	if nbyte != 4 {
		return fmt.Errorf("needed 4 bytes, but got %d bytes", nbyte)
	}

	//  time to ban
	frame_length := binary.LittleEndian.Uint32(frame_length_buf[:])
	if frame_length == 0 {
		return nil
	}
	// most probably memory DDOS attack, kill the connection
	if uint64(frame_length) > (5 * config.STARGATE_HE_MAX_BLOCK_SIZE) {
		return fmt.Errorf("Frame length is too big Expected %d Actual %d", 5*config.STARGATE_HE_MAX_BLOCK_SIZE, frame_length)
	}

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Grow(int(frame_length))
	defer bufPool.Put(buf)

	data_buf := buf.Bytes()
	data_buf = data_buf[:frame_length]
	data_size, err := io.ReadFull(r, data_buf)
	if err != nil || data_size <= 0 || uint32(data_size) != frame_length {
		return fmt.Errorf("Could not read data size  read %d, frame length %d err %s", data_size, frame_length, err)
	}
	data_buf = data_buf[:frame_length]
	err = cbor.Unmarshal(data_buf, obj)

	//fmt.Printf("Read object %+v raw %s\n",obj, data_buf)
	return err
}

// reads our data, length prefix blocks
func Write_Data_Frame_KCP(w net.Conn, obj interface{}) error {
	var frame_length_buf [4]byte
	data_bytes, err := cbor.Marshal(obj)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(frame_length_buf[:], uint32(len(data_bytes)))

	w.SetWriteDeadline(time.Now().Add(WRITE_TIMEOUT))
	if _, err = w.Write(frame_length_buf[:]); err != nil {
		return err
	}
	_, err = w.Write(data_bytes[:])
	//fmt.Printf("Wrote object %+v raw %s\n",obj, data_bytes)
	return err
}

// ClientCodec implements the rpc.ClientCodec interface for generic golang objects.
type ClientCodecKCP struct {
	r net.Conn
	sync.Mutex
}

// NewClientCodec returns a ClientCodec for communicating with the ServerCodec
// on the other end of the conn.
// to support deadlines we use net.conn
func NewCBORCodecKCP(conn net.Conn) *ClientCodecKCP {
	return &ClientCodecKCP{r: conn}
}

// ReadResponseHeader reads a 4 byte length from the connection and decodes that many
// subsequent bytes into the given object, decodes it, and stores the fields
// in the given request.
func (c *ClientCodecKCP) ReadResponseHeader(resp *rpc2.Response) error {
	var header RequestResponse
	if err := Read_Data_Frame_KCP(c.r, &header); err != nil {
		return err
	}
	//if header.Method == "" {
	//	return fmt.Errorf("header missing method: %s", "no Method")
	//}
	//resp.Method = header.Method
	resp.Seq = header.Seq
	resp.Error = header.Error

	return nil
}

// Close closes the underlying connection.
func (c *ClientCodecKCP) Close() error {
	return c.r.Close()
}

// ReadRequestHeader reads the header (which is prefixed by a 4 byte lil endian length
// indicating its size) from the connection, decodes it, and stores the fields
// in the given request.
func (s *ClientCodecKCP) ReadHeader(req *rpc2.Request, resp *rpc2.Response) error {
	var header RequestResponse
	if err := Read_Data_Frame_KCP(s.r, &header); err != nil {
		return err
	}

	if header.Method != "" {
		req.Seq = header.Seq
		req.Method = header.Method
	} else {
		resp.Seq = header.Seq
		resp.Error = header.Error
	}
	return nil
}

// ReadRequestBody reads a 4 byte length from the connection and decodes that many
// subsequent bytes into the object
func (s *ClientCodecKCP) ReadRequestBody(obj interface{}) error {
	if obj == nil {
		return nil
	}
	return Read_Data_Frame_KCP(s.r, obj)
}

// ReadResponseBody reads a 4 byte length from the connection and decodes that many
// subsequent bytes into the given object (which should be a pointer to a
// struct).
func (c *ClientCodecKCP) ReadResponseBody(obj interface{}) error {
	if obj == nil {
		return nil
	}
	return Read_Data_Frame_KCP(c.r, obj)
}

// WriteRequest writes the 4 byte length from the connection and encodes that many
// subsequent bytes into the given object.
func (c *ClientCodecKCP) WriteRequest(req *rpc2.Request, obj interface{}) error {
	c.Lock()
	defer c.Unlock()

	header := RequestResponse{Method: req.Method, Seq: req.Seq}
	if err := Write_Data_Frame_KCP(c.r, header); err != nil {
		return err
	}
	return Write_Data_Frame_KCP(c.r, obj)
}

// WriteResponse writes the appropriate header. If
// the response was invalid, the size of the body of the resp is reported as
// having size zero and is not sent.
func (c *ClientCodecKCP) WriteResponse(resp *rpc2.Response, obj interface{}) error {
	c.Lock()
	defer c.Unlock()
	header := RequestResponse{Seq: resp.Seq, Error: resp.Error}
	if err := Write_Data_Frame_KCP(c.r, header); err != nil {
		return err
	}

	if resp.Error == "" { // only write response object if error is nil
		return Write_Data_Frame_KCP(c.r, obj)
	}

	return nil
}
