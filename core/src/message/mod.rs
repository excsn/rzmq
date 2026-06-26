//! Message-related types (`Msg`, `MsgFlags`, `Metadata`, `Blob`).

mod blob;
mod flags;
mod metadata;
mod msg;

pub use blob::Blob;
pub use flags::MsgFlags;
pub use metadata::Metadata;
pub use msg::Msg;

use std::fmt;
use xs_foundation::collections::vec::u8::VecU8;

enum FrameBatchInner {
  Empty,
  Single(Msg),
  Two(Msg, Msg),
  Many(VecU8<Msg>),
}

/// A stack-optimized batch of message frames for a single logical ZMQ message.
/// Stores up to 2 frames inline without heap allocation; spills to `VecU8` beyond that.
pub struct FrameBatch {
  inner: FrameBatchInner,
}

impl FrameBatch {
  pub fn new() -> Self {
    Self { inner: FrameBatchInner::Empty }
  }

  pub fn with_capacity(capacity: usize) -> Self {
    if capacity <= 2 {
      Self::new()
    } else {
      Self { inner: FrameBatchInner::Many(VecU8::with_capacity(capacity)) }
    }
  }

  pub fn push(&mut self, msg: Msg) {
    let old = std::mem::replace(&mut self.inner, FrameBatchInner::Empty);
    self.inner = match old {
      FrameBatchInner::Empty => FrameBatchInner::Single(msg),
      FrameBatchInner::Single(m1) => FrameBatchInner::Two(m1, msg),
      FrameBatchInner::Two(m1, m2) => {
        let mut vec = VecU8::with_capacity(3);
        vec.push(m1);
        vec.push(m2);
        vec.push(msg);
        FrameBatchInner::Many(vec)
      }
      FrameBatchInner::Many(mut vec) => {
        vec.push(msg);
        FrameBatchInner::Many(vec)
      }
    };
  }

  pub fn pop(&mut self) -> Option<Msg> {
    let old = std::mem::replace(&mut self.inner, FrameBatchInner::Empty);
    match old {
      FrameBatchInner::Empty => None,
      FrameBatchInner::Single(m) => Some(m),
      FrameBatchInner::Two(m1, m2) => {
        self.inner = FrameBatchInner::Single(m1);
        Some(m2)
      }
      FrameBatchInner::Many(mut vec) => {
        let result = vec.pop();
        self.inner = demote(vec);
        result
      }
    }
  }

  pub fn len(&self) -> usize {
    match &self.inner {
      FrameBatchInner::Empty => 0,
      FrameBatchInner::Single(_) => 1,
      FrameBatchInner::Two(_, _) => 2,
      FrameBatchInner::Many(v) => v.len(),
    }
  }

  pub fn is_empty(&self) -> bool {
    matches!(self.inner, FrameBatchInner::Empty)
  }

  pub fn insert(&mut self, index: usize, msg: Msg) {
    let old = std::mem::replace(&mut self.inner, FrameBatchInner::Empty);
    self.inner = match old {
      FrameBatchInner::Empty => {
        assert_eq!(index, 0, "index out of bounds");
        FrameBatchInner::Single(msg)
      }
      FrameBatchInner::Single(m1) => match index {
        0 => FrameBatchInner::Two(msg, m1),
        1 => FrameBatchInner::Two(m1, msg),
        _ => panic!("index out of bounds"),
      },
      FrameBatchInner::Two(m1, m2) => {
        let mut vec = VecU8::with_capacity(3);
        match index {
          0 => {
            vec.push(msg);
            vec.push(m1);
            vec.push(m2);
          }
          1 => {
            vec.push(m1);
            vec.push(msg);
            vec.push(m2);
          }
          2 => {
            vec.push(m1);
            vec.push(m2);
            vec.push(msg);
          }
          _ => panic!("index out of bounds"),
        }
        FrameBatchInner::Many(vec)
      }
      FrameBatchInner::Many(mut vec) => {
        vec.insert(index, msg);
        FrameBatchInner::Many(vec)
      }
    };
  }

  pub fn remove(&mut self, index: usize) -> Msg {
    let old = std::mem::replace(&mut self.inner, FrameBatchInner::Empty);
    match old {
      FrameBatchInner::Empty => panic!("index out of bounds"),
      FrameBatchInner::Single(m) => {
        assert_eq!(index, 0, "index out of bounds");
        m
      }
      FrameBatchInner::Two(m1, m2) => match index {
        0 => {
          self.inner = FrameBatchInner::Single(m2);
          m1
        }
        1 => {
          self.inner = FrameBatchInner::Single(m1);
          m2
        }
        _ => panic!("index out of bounds"),
      },
      FrameBatchInner::Many(mut vec) => {
        let result = vec.remove(index);
        self.inner = demote(vec);
        result
      }
    }
  }

  pub fn first(&self) -> Option<&Msg> {
    if self.is_empty() { None } else { Some(&self[0]) }
  }

  pub fn last_mut(&mut self) -> Option<&mut Msg> {
    let len = self.len();
    if len == 0 { None } else { Some(&mut self[len - 1]) }
  }

  pub fn iter(&self) -> FrameBatchIter<'_> {
    FrameBatchIter { batch: self, index: 0 }
  }

  pub fn iter_mut(&mut self) -> FrameBatchIterMut<'_> {
    FrameBatchIterMut { batch: self, index: 0 }
  }
}

/// Demote a `VecU8<Msg>` to the smallest fitting inline variant.
fn demote(mut vec: VecU8<Msg>) -> FrameBatchInner {
  match vec.len() {
    0 => FrameBatchInner::Empty,
    1 => FrameBatchInner::Single(vec.remove(0)),
    2 => {
      let m1 = vec.remove(0);
      let m2 = vec.remove(0);
      FrameBatchInner::Two(m1, m2)
    }
    _ => FrameBatchInner::Many(vec),
  }
}

// --- Trait Implementations ---

impl Default for FrameBatch {
  fn default() -> Self {
    Self::new()
  }
}

impl Clone for FrameBatch {
  fn clone(&self) -> Self {
    match &self.inner {
      FrameBatchInner::Empty => FrameBatch::new(),
      FrameBatchInner::Single(m) => FrameBatch { inner: FrameBatchInner::Single(m.clone()) },
      FrameBatchInner::Two(m1, m2) => {
        FrameBatch { inner: FrameBatchInner::Two(m1.clone(), m2.clone()) }
      }
      FrameBatchInner::Many(v) => {
        let mut vec = VecU8::with_capacity(v.len());
        for m in v.iter() {
          vec.push(m.clone());
        }
        FrameBatch { inner: FrameBatchInner::Many(vec) }
      }
    }
  }
}

impl fmt::Debug for FrameBatch {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_list().entries(self.iter()).finish()
  }
}

impl std::ops::Index<usize> for FrameBatch {
  type Output = Msg;

  fn index(&self, index: usize) -> &Self::Output {
    match &self.inner {
      FrameBatchInner::Empty => panic!("index out of bounds"),
      FrameBatchInner::Single(m) => {
        if index == 0 { m } else { panic!("index out of bounds") }
      }
      FrameBatchInner::Two(m1, m2) => match index {
        0 => m1,
        1 => m2,
        _ => panic!("index out of bounds"),
      },
      FrameBatchInner::Many(v) => &v[index],
    }
  }
}

impl std::ops::IndexMut<usize> for FrameBatch {
  fn index_mut(&mut self, index: usize) -> &mut Self::Output {
    match &mut self.inner {
      FrameBatchInner::Empty => panic!("index out of bounds"),
      FrameBatchInner::Single(m) => {
        if index == 0 { m } else { panic!("index out of bounds") }
      }
      FrameBatchInner::Two(m1, m2) => match index {
        0 => m1,
        1 => m2,
        _ => panic!("index out of bounds"),
      },
      FrameBatchInner::Many(v) => &mut v[index],
    }
  }
}

impl Extend<Msg> for FrameBatch {
  fn extend<T: IntoIterator<Item = Msg>>(&mut self, iter: T) {
    for msg in iter {
      self.push(msg);
    }
  }
}

impl From<Vec<Msg>> for FrameBatch {
  fn from(mut v: Vec<Msg>) -> Self {
    match v.len() {
      0 => FrameBatch::new(),
      1 => FrameBatch { inner: FrameBatchInner::Single(v.remove(0)) },
      2 => {
        let m1 = v.remove(0);
        let m2 = v.remove(0);
        FrameBatch { inner: FrameBatchInner::Two(m1, m2) }
      }
      _ => {
        let mut vec = VecU8::with_capacity(v.len());
        for m in v {
          vec.push(m);
        }
        FrameBatch { inner: FrameBatchInner::Many(vec) }
      }
    }
  }
}

impl From<FrameBatch> for Vec<Msg> {
  fn from(batch: FrameBatch) -> Self {
    match batch.inner {
      FrameBatchInner::Empty => Vec::new(),
      FrameBatchInner::Single(m) => vec![m],
      FrameBatchInner::Two(m1, m2) => vec![m1, m2],
      FrameBatchInner::Many(v) => v.into_iter().collect(),
    }
  }
}

impl IntoIterator for FrameBatch {
  type Item = Msg;
  type IntoIter = std::vec::IntoIter<Msg>;

  fn into_iter(self) -> Self::IntoIter {
    let v: Vec<Msg> = self.into();
    v.into_iter()
  }
}

impl<'a> IntoIterator for &'a FrameBatch {
  type Item = &'a Msg;
  type IntoIter = FrameBatchIter<'a>;

  fn into_iter(self) -> Self::IntoIter {
    self.iter()
  }
}

// --- Iterator Types ---

pub struct FrameBatchIter<'a> {
  batch: &'a FrameBatch,
  index: usize,
}

impl<'a> Iterator for FrameBatchIter<'a> {
  type Item = &'a Msg;

  fn next(&mut self) -> Option<Self::Item> {
    if self.index < self.batch.len() {
      let item = &self.batch[self.index];
      self.index += 1;
      Some(item)
    } else {
      None
    }
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    let rem = self.batch.len() - self.index;
    (rem, Some(rem))
  }
}

impl ExactSizeIterator for FrameBatchIter<'_> {}

pub struct FrameBatchIterMut<'a> {
  batch: &'a mut FrameBatch,
  index: usize,
}

impl<'a> Iterator for FrameBatchIterMut<'a> {
  type Item = &'a mut Msg;

  fn next(&mut self) -> Option<Self::Item> {
    if self.index >= self.batch.len() {
      return None;
    }
    let idx = self.index;
    self.index += 1;
    // Safety: index advances monotonically so no two calls yield the same element.
    let ptr: *mut Msg = &mut self.batch[idx];
    Some(unsafe { &mut *ptr })
  }
}
