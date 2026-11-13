use std::fs::ReadDir;

use super::state::SafeDir;

pub(super) const MAX_ENTRIES_PER_PASS: usize = 512;

#[derive(Default)]
pub(super) struct MaterialCursor {
    directories: Option<ReadDir>,
    children: Option<(String, ReadDir)>,
}

pub(super) enum Entry {
    Directory(String),
    Material(String, String),
    FinishedDirectory(String),
}

impl MaterialCursor {
    pub(super) fn next(&mut self, root: &SafeDir) -> std::io::Result<Option<Entry>> {
        if let Some((id, children)) = self.children.as_mut() {
            if let Some(entry) = children.next() {
                return Ok(Some(Entry::Material(id.clone(), name(entry?)?)));
            }
            let (id, _) = self.children.take().expect("current directory checked");
            return Ok(Some(Entry::FinishedDirectory(id)));
        }
        if self.directories.is_none() {
            self.directories = Some(std::fs::read_dir(root.path())?);
        }
        match self.directories.as_mut().expect("root initialized").next() {
            Some(entry) => Ok(Some(Entry::Directory(name(entry?)?))),
            None => {
                self.directories = None;
                Ok(None)
            }
        }
    }

    pub(super) fn visit(&mut self, id: String, directory: &SafeDir) -> std::io::Result<()> {
        self.children = Some((id, std::fs::read_dir(directory.path())?));
        Ok(())
    }

    pub(super) fn skip_directory(&mut self) {
        self.children = None;
    }
}

fn name(entry: std::fs::DirEntry) -> std::io::Result<String> {
    entry.file_name().into_string().map_err(|_| std::io::Error::other("invalid material entry name"))
}
