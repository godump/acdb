package ccdb

type Option struct {
	Command string `json:"command"`
	K       string `json:"k"`
	V       []byte `json:"v"`
}

type Output struct {
	Err string `json:"err"`
	K   string `json:"k"`
	V   []byte `json:"v"`
}

// func NewEmerge(server string) *Emerge {
// 	return &Emerge{
// 		server: server,
// 		client: http.DefaultClient,
// 	}
// }

// type Emerge struct {
// 	server string
// 	client *http.Client
// }

// func (e *Emerge) Cmd(option *Option) *Output {
// 	output := &Output{}
// 	buf, err := json.Marshal(option)
// 	if err != nil {
// 		output.Err = err.Error()
// 		return output
// 	}
// 	req, err := http.NewRequest("PUT", e.server, bytes.NewReader(buf))
// 	if err != nil {
// 		output.Err = err.Error()
// 		return output
// 	}
// 	res, err := e.client.Do(req)
// 	if err != nil {
// 		output.Err = err.Error()
// 		return output
// 	}
// 	defer res.Body.Close()
// 	if res.StatusCode != 200 {
// 		output.Err = errors.New(strconv.Itoa(res.StatusCode))
// 		return output
// 	}
// 	if err := json.NewDecoder(res.Body).Decode(output); err != nil {
// 		output.Err = err.Error()
// 		return output
// 	}
// 	return output
// }

// func (e *Emerge) Get(k string, v interface{}) error {
// 	output := e.Cmd(&Option{Command: "GET", K: k})
// 	if err != nil {
// 		return err
// 	}
// 	return json.Unmarshal(output.V, v)
// }

// func (e *Emerge) Set(k string, v interface{}) error {
// 	buf, err := json.Marshal(v)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = e.Cmd(&Option{Command: "SET", K: k, V: buf})
// 	return err
// }

// func (e *Emerge) Del(k string) {
// 	option := &Option{Command: "DEL", K: k}
// 	e.Cmd(option)
// }

// func (e *Emerge) Add(k string, n int64) error {
// 	buf, _ := json.Marshal(n)
// 	option := &Option{Command: "ADD", K: k, V: buf}
// 	_, err := e.Cmd(option)
// 	return err
// }

// func (e *Emerge) Dec(k string, n int64) error {
// 	buf, _ := json.Marshal(n)
// 	option := &Option{Command: "DEC", K: k, V: buf}
// 	_, err := e.Cmd(option)
// 	return err
// }

// func Cli(server string) acdb.Client {
// 	return NewEmerge(server)
// }
