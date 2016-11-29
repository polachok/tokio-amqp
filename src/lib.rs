extern crate tokio_core;
extern crate futures;
extern crate amqp;

use tokio_core::net::{TcpStream,TcpStreamNew};
use tokio_core::reactor::Core;
use tokio_core::io::Io;
use std::net::SocketAddr;
use std::str::FromStr;
use std::io::{Read,Write,Cursor};
use futures::Future;
use amqp::protocol::{Frame, FrameType};


struct AmqpStream {
    tcp: TcpStream,
}

struct AmqpStreamNew {
    tcp: TcpStreamNew,
}

impl Future for AmqpStreamNew {
    type Item = AmqpStream;
    type Error = std::io::Error;

    fn poll(&mut self) -> futures::Poll<AmqpStream, std::io::Error> {
        match self.tcp.poll() {
            Ok(tcp) => Ok(tcp.map(|tcp| AmqpStream { tcp: tcp })),
            Err(e) => Err(e),
        }
    }
}

impl AmqpStream {
    pub fn connect(addr: &SocketAddr, handle: &tokio_core::reactor::Handle) -> AmqpStreamNew {
       AmqpStreamNew { tcp: TcpStream::connect(addr, handle) }
    }
}

struct AmqpTransport<T> {
    inner: T,
    read_buffer: Vec<u8>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }

    #[test]
    fn conn_test() {
        use tokio_core::net::TcpStream;
        use tokio_core::reactor::Core;
        use tokio_core::io::Io;
        use tokio_core::io;
        use std::net::SocketAddr;
        use std::str::FromStr;
        use std::io::{Read,Write,Cursor};
        use futures::Future;
        use futures;
        use amqp::protocol::{self, Frame, FrameType, MethodFrame, Method};

        fn do_conn(stream: &mut TcpStream) {
            println!("{:?}", stream);
            stream.write_all(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1]);
        }
       
        let mut l = Core::new().unwrap();
        let handle = l.handle();

        let addr = SocketAddr::from_str("192.168.0.222:5672").unwrap();
        //let addr = SocketAddr::from_str("127.0.0.1:9999").unwrap();

        let stream = TcpStream::connect(&addr, &handle);
        let mut buf = vec![0; 1024];

        let f = stream.and_then(|mut s| {
            do_conn(&mut s);
            Ok(s)
        }).and_then(|mut s| {
            io::read(s, &mut buf)
        }).and_then(|(r,bs,_)| {
            //println!("END {:?}", bs);
            let mut c = Cursor::new(bs);
            let f = Frame::decode(&mut c).unwrap();
            println!("FRAME {:?}", f);
            let mf = MethodFrame::decode(&f).unwrap();
            println!("METHOD FRAME {:?} name: {}", mf, mf.method_name());
            let met: protocol::connection::Start = Method::decode(mf).unwrap();
            println!("METHOD {:?}", met);
            Ok(1)
         });
        l.run(f).unwrap();
    }
}
