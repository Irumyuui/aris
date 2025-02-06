use super::RecordType;

struct Record {
    ty: RecordType,
    data: Vec<u8>,
}

pub struct LogReader {
    fd: std::fs::File,
    cache: Vec<Vec<u8>>,
    ring: rio::Rio,
}

// impl LogReader {
//     pub fn new(fd: std::fs::File, cache_len: usize) -> Self {
//         todo!()
//     }

//     fn read_raw_datas(&mut self) {
//         let comps = Vec::with_capacity(self.cache.len());
//         for i in 0..self.cache.len() {
//             let comp = self.ring.read_at(&self.fd, &self.cache[i]);
//         }

//         todo!()
//     }
// }
