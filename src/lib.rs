extern crate tokio_core;
extern crate futures;
extern crate amqp;
extern crate tokio_proto;
extern crate tokio_service;

use tokio_core::net::{TcpStream,TcpStreamNew};
use tokio_core::reactor::Core;
use tokio_core::io::Io;
use tokio_core::io::Codec;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio_core::io::{EasyBuf,Framed};
use std::io::{Read,Write,Cursor};
use futures::{Future,Stream,Sink};
use amqp::protocol::{self, Frame, FrameType, MethodFrame, Method};
use amqp::AMQPError;
use tokio_proto::multiplex::ClientProto;
use tokio_service::Service;

struct AmqpClient<T> where T: tokio_service::Service {
    inner: T
}

impl<T: tokio_service::Service<Request = Frame, Response = Frame, Error = AMQPError> + 'static> AmqpClient<T> {
    fn test(&self) -> Result<i32, AMQPError> {
        Ok(1)
    }

    pub fn open_channel(&mut self, chan: u16) -> T::Future {
        use amqp::protocol;
        let method = protocol::channel::Open { out_of_band: "".to_owned() };
        let f = Frame {
                frame_type: amqp::protocol::FrameType::METHOD,
                channel: 0,
                payload: method.encode_method_frame().unwrap(),
        };
        self.inner.call(f)
    }
}

struct AmqpProto {
    options: amqp::Options,
 }

impl ClientProto<TcpStream> for AmqpProto {
    type Request = Frame;
    type Response = Frame;
    type Error = amqp::AMQPError;
    type Transport = AmqpStream<TcpStream>;
    type BindTransport = Box<Future<Item = Self::Transport, Error = std::io::Error>>;
    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        static bytes: [u8; 8] = [b'A', b'M', b'Q', b'P', 0, 0, 9, 1];
        let options2 = amqp::Options {
                    host: self.options.host.clone(),
                    port: self.options.port.clone(),
                    vhost: self.options.vhost.clone(),
                    login: self.options.login.clone(),
                    password: self.options.password.clone(),
                    frame_max_limit: self.options.frame_max_limit.clone(),
                    channel_max_limit: 65535,
                    locale: self.options.locale.clone(),
                    scheme: amqp::AMQPScheme::AMQP,
                };


        let f = tokio_core::io::write_all(io, &bytes)
                    .and_then(move |(tcp, _)| Ok(AmqpStream {
                        inner: tcp.framed(AmqpCodec {}),
                        frame_max_limit: 131072,
                        channel_max_limit: 65535,
                        channels: Vec::new(),
                    })).and_then(|stream| stream.auth(options2));
        Box::new(f)
    }
}


struct AmqpCodec { }

impl Codec for AmqpCodec {
    type In = (u64, Frame);
    type Out = (u64, Frame);

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, std::io::Error> {
        //println!("DECODING {:?}", buf.as_slice());
        let used = {
            let bytes = buf.as_slice();
            if let Some(pos) = bytes.iter().position(|b| *b == 0xce) {
                let mut c = Cursor::new(&bytes[0..pos+1]);
                let f = Frame::decode(&mut c).ok();
                if let Some(f) = f {
                    //println!("FRAME {:?}", f);
                    //let mf = MethodFrame::decode(&f).unwrap();
                    //println!("METHOD FRAME {:?} name: {}", mf, mf.method_name());
                    //let met: protocol::connection::Start = Method::decode(mf).unwrap();
                    //println!("METHOD {:?}", met);
                    Some((pos, Some((f.channel as u64, f))))
                } else {
                    None
                }
                //Some((pos, f))
            } else {
                //println!("NOTHING IN HERE");
                None
            }
        };
        if let Some((pos, f)) = used {
            buf.drain_to(pos+1);
            return Ok(f)
        }
        Ok(None)
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> std::io::Result<()> {
        let mut frame = msg.1.clone();
        frame.channel = msg.0 as u16;
        let mut v = frame.encode().unwrap();
        buf.append(&mut v);
        Ok(())
    }
}

#[derive(Debug)]
struct Channel {
    id: u16,
}

impl Channel {
    fn split_content_into_frames(content: Vec<u8>, frame_limit: u32) -> Vec<Vec<u8>> {
        use std::cmp;
        assert!(frame_limit > 0, "Can't have frame_max_limit == 0");
        let mut content_frames = vec![];
        let mut current_pos = 0;
        while current_pos < content.len() {
            let new_pos = current_pos + cmp::min(content.len() - current_pos, frame_limit as usize);
            content_frames.push(content[current_pos..new_pos].to_vec());
            current_pos = new_pos;
        }
        content_frames
    }

    fn do_write<S: Sink<SinkItem = Frame, SinkError = amqp::AMQPError>>(sink: S, frame: Frame)
        -> futures::sink::Send<S> {
        let frame_max_limit = 131072;
        println!("FRAMETYPE {:?}", frame.frame_type);
        match frame.frame_type {
                /*
            FrameType::BODY => {
                // TODO: Check if need to include frame header + end octet into calculation. (9
                // bytes extra)
                for content_frame in Self::split_content_into_frames(frame.payload,
                                                               frame_max_limit)
                    .into_iter() {
                    sink.send(Frame {
                        frame_type: frame.frame_type,
                        channel: frame.channel,
                        payload: content_frame,
                    })
                }
            }
                */
            _ => sink.send(frame),
        }
    }

    pub fn send_method_frame<M, S>(sink: S, chan_id: u16, method: &M) -> futures::sink::Send<S>
        where M: amqp::protocol::Method, S: Sink<SinkItem = Frame, SinkError = amqp::AMQPError>
        {
            println!("Sending method {} to channel {}", method.name(), chan_id);
            Self::do_write(sink, Frame {
                frame_type: amqp::protocol::FrameType::METHOD,
                channel: chan_id,
                payload: method.encode_method_frame().unwrap(),
            })
        }
}

struct AmqpStream<T: tokio_core::io::Io> {
    inner: Framed<T, AmqpCodec>,
    channel_max_limit: u16,
    frame_max_limit: u32,
    channels: Vec<Channel>,
}

/*
struct AmqpStreamNew {
    tcp: TcpStream,
}

impl Future for AmqpStreamNew {
    type Item = AmqpStream<TcpStream>;
    type Error = std::io::Error;

    fn poll(&mut self) -> futures::Poll<AmqpStream<TcpStream>, std::io::Error> {
        use futures::Async;
        use futures::Poll;
        use tokio_core::io;
        /*
        match self.tcp.poll() {
            Ok(Async::Ready(mut tcp)) => {
                */
                io::write_all(&mut self.tcp, &[b'A', b'M', b'Q', b'P', 0, 0, 9, 1])
                    .map(|(tcp, _)| AmqpStream {
                        inner: tcp.framed(AmqpCodec {}),
                        frame_max_limit: 131072,
                        channel_max_limit: 65535,
                        channels: Vec::new(),
                    })
                    .poll()
/*
                //Ok(Async::Ready(AmqpStream { inner: tcp.framed(AmqpCodec {}) }))
            },
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
        */
    }
}
*/

/*
impl AmqpStream<TcpStream> {
    pub fn connect(addr: &SocketAddr, handle: &tokio_core::reactor::Handle) -> AmqpStreamNew {
       AmqpStreamNew { tcp: TcpStream::connect(addr, handle) }
    }
}
*/

impl<T> Read for AmqpStream<T> where T: tokio_core::io::Io {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let inn = self.inner.get_mut();
        inn.read(buf)
    }
}

impl<T> Write for AmqpStream<T> where T: tokio_core::io::Io {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let inn = self.inner.get_mut();
        inn.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let inn = self.inner.get_mut();
        inn.flush()
    }
}

impl<T> futures::Stream for AmqpStream<T> where T: tokio_core::io::Io {
    type Item = (u64, Frame);
    type Error = std::io::Error;

    fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
        self.inner.poll()
    }
}

impl<T> futures::Sink for AmqpStream<T> where T: tokio_core::io::Io {
    type SinkItem = (u64, Frame);
    type SinkError = std::io::Error;

    fn start_send(&mut self,
              item: Self::SinkItem)
              -> futures::StartSend<Self::SinkItem, Self::SinkError> {
                  self.inner.start_send(item)
              }
    fn poll_complete(&mut self) -> futures::Poll<(), Self::SinkError> {
        self.inner.poll_complete()
    }
}

impl<T> AmqpStream<T> where T: tokio_core::io::Io + 'static {
    /*
    fn tune(self) -> Box<Future<Item = Self, Error = std::io::Error>> {
        let stream = self.into_future().and_then(|(frame, mut stream)| {
            if let Some(ref frame) = frame {
                let method_frame = MethodFrame::decode(&frame).unwrap();
                let tune: protocol::connection::Tune = match method_frame.method_name() {
                    "connection.tune" => protocol::Method::decode(method_frame).unwrap(),
                    "connection.close" => panic!("CLOSE RECEIVED"),
                    meth => panic!("wtf"), // return Err(AMQPError::Protocol(format!("Unexpected method frame: {:?}", meth))),
                };
                println!("tune {:?}", tune);
                stream.channel_max_limit = std::cmp::min(tune.channel_max, stream.channel_max_limit);
                stream.frame_max_limit = std::cmp::min(tune.frame_max, stream.frame_max_limit);
            }
            //Ok((sink, stream))
            Ok(stream)
        }).map_err(|(e, _)| {
            e
        });

        let stream = stream.and_then(|stream| {
            let tune_ok = protocol::connection::TuneOk {
                channel_max: stream.channel_max_limit,
                frame_max: stream.frame_max_limit,
                heartbeat: 60,
            };
            println!("SENDING TUNE OK");
            Channel::send_method_frame(stream, 0, &tune_ok)
            .and_then(|stream| {
                let open = protocol::connection::Open {
                    virtual_host: "vm-apolyakov".to_owned(), //percent_decode(&options.vhost),
                    capabilities: "".to_owned(),
                    insist: false,
                };
                println!("Sending connection.open: {:?}", open);
                Channel::send_method_frame(stream, 0, &open)
            }).and_then(|stream| {
                stream.into_future().map_err(|(e, _)| e).and_then(|(frame, stream)| {
                    if let Some(ref frame) = frame {
                        let method_frame = try!(MethodFrame::decode(&frame));
                        let open_ok: protocol::connection::OpenOk = match method_frame.method_name() {
                            "connection.open-ok" => try!(protocol::Method::decode(method_frame)),
                            meth => return Err(AMQPError::Protocol(format!("Unexpected method frame: {:?}", meth))),
                        };
                        println!("GOT FRAME {:?}", open_ok);
                    }
                    Ok(stream)
                })
            })});
        Box::new(stream)
    }
    */

    pub fn auth(mut self, options: amqp::Options) -> Box<Future<Item = Self, Error = std::io::Error>> {
        use amqp::Options;

        let chan0 = Channel { id: 0 };
        self.channels.push(chan0);

        fn conn_start_frame(options: amqp::Options) -> amqp::protocol::connection::StartOk {
            use amqp::protocol::Table;
            use amqp::protocol::{FieldTable, Bool, LongString};

            let mut client_properties = Table::new();
            let mut capabilities = Table::new();
            capabilities.insert("publisher_confirms".to_owned(), Bool(true));
            capabilities.insert("consumer_cancel_notify".to_owned(), Bool(true));
            capabilities.insert("exchange_exchange_bindings".to_owned(), Bool(true));
            capabilities.insert("basic.nack".to_owned(), Bool(true));
            capabilities.insert("connection.blocked".to_owned(), Bool(true));
            capabilities.insert("authentication_failure_close".to_owned(), Bool(true));
            client_properties.insert("capabilities".to_owned(), FieldTable(capabilities));
            client_properties.insert("product".to_owned(), LongString("rust-amqp".to_owned()));
            client_properties.insert("platform".to_owned(), LongString("rust".to_owned()));
            client_properties.insert("version".to_owned(), LongString("0.0.1".to_owned()));
            client_properties.insert("information".to_owned(),
            LongString("https://github.com/Antti/rust-amqp".to_owned()));

            let start_ok = protocol::connection::StartOk {
                client_properties: client_properties,
                mechanism: "PLAIN".to_owned(),
                response: format!("\0{}\0{}", options.login, options.password),
                locale: options.locale.to_owned(),
            };
            start_ok
        }

        let stream = self.into_future().map_err(|(e, _)| e).and_then(|(frame, stream)| (move || {
            if let Some(ref frame) = frame {
                let ref frame = frame.1;
                let method_frame = try!(MethodFrame::decode(&frame));
                let start: protocol::connection::Start = match method_frame.method_name() {
                    "connection.start" => try!(protocol::Method::decode(method_frame)),
                    meth => return Err(AMQPError::Protocol(format!("Unexpected method frame: {:?}", meth))),
                };
                println!("start {:?}", start);
            }
            Ok(stream)
        })().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        );
/*
        let stream = stream.and_then(|stream| {
            Channel::send_method_frame(stream, 0, &conn_start_frame(options))
        }).and_then(|stream| {
            stream.tune()
        });
        */
        //let stream = stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
        Box::new(stream)
    }
/*
    pub fn channel_open(mut self) -> Box<Future<Item = Self, Error = amqp::AMQPError>> {
        let open = protocol::channel::Open { out_of_band: "".to_owned() };
        let chan_id = self.channels.len() as u16;

        let f = Channel::send_method_frame(self, chan_id, &open)
            .and_then(move |stream| { 
                stream.into_future()
                    .map_err(|(e, _)| e)
                    .and_then(move |(frame, mut stream)| {
                        if let Some(ref frame) = frame {
                            let method_frame = try!(MethodFrame::decode(&frame));
                            println!("METHOD FRAME: {:?}", method_frame);
                            let openOk: protocol::channel::OpenOk = match method_frame.method_name() {
                                "channel.open-ok" => try!(protocol::Method::decode(method_frame)),
                                meth => return Err(AMQPError::Protocol(format!("Unexpected method frame: {:?}", meth))),
                            };
                            println!("CHAN OPEN OK {:?}", openOk);
                            let chan = Channel { id: chan_id };
                            stream.channels.push(chan);
                        }
                        Ok(stream)
                    })
            });
        Box::new(f)
    }

    pub fn declare_queue(self, queue: String) -> Box<Future<Item = Self, Error = amqp::AMQPError>> {
        let declare = protocol::queue::Declare {
            ticket: 0,
            queue: queue.into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: amqp::Table::new(),
        };
        let f = Channel::send_method_frame(self, 1, &declare)
            .and_then(|stream| { 
                stream.into_future()
                    .map_err(|(e, _)| e)
                    .and_then(|(frame, stream)| {
                        if let Some(ref frame) = frame {
                            let method_frame = try!(MethodFrame::decode(&frame));
                            println!("METHOD FRAME: {:?}", method_frame);
                            let declareOk: protocol::queue::DeclareOk = match method_frame.method_name() {
                                "queue.declare-ok" => try!(protocol::Method::decode(method_frame)),
                                meth => return Err(AMQPError::Protocol(format!("Unexpected method frame: {:?}", meth))),
                            };
                            println!("DECLARE OK {:?}", declareOk);
                        }
                        Ok(stream)
                    })
            });
        Box::new(f)

    }

    pub fn consume(self, queue: String) -> Box<Future<Item = Self, Error = amqp::AMQPError>> {
        let consume = amqp::protocol::basic::Consume {
            ticket: 0,
            queue: queue.into(),
            consumer_tag: "".into(),
            no_local: false,
            no_ack: false,
            exclusive: false,
            nowait: false,
            arguments: amqp::Table::new(),
        };
        let f = Channel::send_method_frame(self, 1, &consume)
            .and_then(|stream| { 
                stream.into_future()
                    .map_err(|(e, _)| e)
                    .and_then(|(frame, stream)| {
                        if let Some(ref frame) = frame {
                            let method_frame = try!(MethodFrame::decode(&frame));
                            println!("METHOD FRAME: {:?}", method_frame);
                            let consumeOk: protocol::basic::ConsumeOk = match method_frame.method_name() {
                                "basic.consume-ok" => try!(protocol::Method::decode(method_frame)),
                                meth => return Err(AMQPError::Protocol(format!("Unexpected method frame: {:?}", meth))),
                            };
                            println!("CONSUME OK {:?}", consumeOk);
                        }
                        Ok(stream)
                    })
            });
        Box::new(f)
    }

    pub fn deliver(self) -> Box<Future<Item = Option<Frame>, Error = amqp::AMQPError>> {
        let f = self.into_future().map_err(|(e, _)| e).and_then(|(frame, _)| {
            if let Some(ref frame) = frame {
                let method_frame = try!(MethodFrame::decode(&frame));
                println!("METHOD FRAME: {:?}", method_frame);
                let deliver: protocol::basic::Deliver = match method_frame.method_name() {
                    "basic.deliver" => try!(protocol::Method::decode(method_frame)),
                    meth => return Err(AMQPError::Protocol(format!("Unexpected method frame: {:?}", meth))),
                };
                println!("DELIVERED {:?}", deliver);
            }
            Ok(frame)
        });

        Box::new(f)
    }
    */
}

struct AmqpTransport<T> {
    inner: T,
    read_buffer: Vec<u8>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn amqp_client() {
        use std::env;
        use tokio_core::net::TcpStream;
        use tokio_core::reactor::Core;
        use tokio_core::io::Io;
        use tokio_core::io;
        use tokio_proto::TcpClient;
        use tokio_service::Service;
        use std::net::SocketAddr;
        use std::str::FromStr;
        use std::io::{Read,Write,Cursor};
        use futures::{Future,Stream};
        use futures;
        use amqp::protocol::{self, Frame, FrameType, MethodFrame, Method};
        use ::{AmqpStream,AmqpProto,AmqpClient};
        use amqp;

        let AMQP_VHOST = env::var("AMQP_VHOST").unwrap();
        let AMQP_LOGIN = env::var("AMQP_LOGIN").unwrap();
        let AMQP_PASSWORD = env::var("AMQP_PASSWORD").unwrap();
        let AMQP_QUEUE = env::var("AMQP_QUEUE").unwrap();

        let mut l = Core::new().unwrap();
        let handle = l.handle();
        let options = amqp::Options {
                    host: "127.0.0.1".to_string(),
                    port: 5672,
                    vhost: AMQP_VHOST,
                    login: AMQP_LOGIN,
                    password: AMQP_PASSWORD,
                    frame_max_limit: 131072,
                    channel_max_limit: 65535,
                    locale: "en_US".to_string(),
                    scheme: amqp::AMQPScheme::AMQP,
                };

        let addr = SocketAddr::from_str("192.168.0.222:5672").unwrap();
        let conn = TcpClient::new(AmqpProto{
                options: options
            })
            .connect(&addr, &handle)
            .map_err(|e| amqp::AMQPError::IoError(e.kind()))
            .map(|client| AmqpClient { inner: client })
            .and_then(|mut client| {
                client.open_channel(1)
            });
        let x = l.run(conn);
        println!("{:?}", x);
    }
    /*
    fn amqp_stream() {
        use std::env;
        use tokio_core::net::TcpStream;
        use tokio_core::reactor::Core;
        use tokio_core::io::Io;
        use tokio_core::io;
        use std::net::SocketAddr;
        use std::str::FromStr;
        use std::io::{Read,Write,Cursor};
        use futures::{Future,Stream};
        use futures;
        use amqp::protocol::{self, Frame, FrameType, MethodFrame, Method};
        use ::AmqpStream;
        use amqp;

        let AMQP_VHOST = env::var("AMQP_VHOST").unwrap();
        let AMQP_LOGIN = env::var("AMQP_LOGIN").unwrap();
        let AMQP_PASSWORD = env::var("AMQP_PASSWORD").unwrap();
        let AMQP_QUEUE = env::var("AMQP_QUEUE").unwrap();

        let mut l = Core::new().unwrap();
        let handle = l.handle();

        let addr = SocketAddr::from_str("192.168.0.222:5672").unwrap();

        let stream = AmqpStream::connect(&addr, &handle);
        let mut buf = vec![0; 1024];

        let f = stream
            .map_err(|e| amqp::AMQPError::IoError(e.kind()))
            .and_then(|amqp| {
                let options = amqp::Options {
                    host: "127.0.0.1".to_string(),
                    port: 5672,
                    vhost: AMQP_VHOST,
                    login: AMQP_LOGIN,
                    password: AMQP_PASSWORD,
                    frame_max_limit: 131072,
                    channel_max_limit: 65535,
                    locale: "en_US".to_string(),
                    scheme: amqp::AMQPScheme::AMQP,
                };

                amqp.auth(options)
            }).and_then(|amqp| amqp.channel_open())
              .and_then(|amqp| amqp.declare_queue(AMQP_QUEUE.clone()))
              .and_then(|amqp| amqp.consume(AMQP_QUEUE.clone()))
              .and_then(|amqp| amqp.deliver()
            );
        let res = l.run(f).unwrap();
        //println!("RESULT: {:?}", res);
    }
    */
}
