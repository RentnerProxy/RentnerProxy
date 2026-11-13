use std::{
    collections::{BTreeMap, BTreeSet},
    io::ErrorKind,
};

use super::super::tls_gc_cursor::{Entry, MAX_ENTRIES_PER_PASS};
use super::recovery::candidate_is_valid;
use super::validation::{index_is_valid, read_regular_private_file};
use super::{
    CertificateError, CertificateIndex, CertificateStore, SafeDir, StoredCertificateCandidate,
    is_canonical_uuid_v7,
};

const MAX_REMOVALS_PER_PASS: usize = 64;

impl CertificateStore {
    pub(crate) async fn collect_garbage(&self) -> Result<(), CertificateError> {
        self.ensure_ready().await?;
        let leases = self.leases.lock().await;
        let index = self.index.lock().await;
        let root = self.certificates_dir()?;
        let mut protected = BTreeMap::<String, BTreeSet<String>>::new();
        protect_index(&index, &mut protected);
        let durable = read_regular_private_file(
            &root,
            super::CERTIFICATE_INDEX_FILE,
            super::MAX_CERTIFICATE_INDEX_BYTES,
        )?
        .map(|bytes| {
            serde_json::from_slice::<CertificateIndex>(&bytes)
                .map_err(|_| CertificateError::StoreUnavailable)
        })
        .transpose()?;
        if let Some(durable) = durable.as_ref() {
            if !index_is_valid(durable) {
                return Err(CertificateError::StoreUnavailable);
            }
            protect_index(durable, &mut protected);
        }
        let mut remaining = MAX_REMOVALS_PER_PASS;
        let mut cursor = self.gc_cursor.lock().await;
        let mut manifests_checked = BTreeSet::new();
        let mut retired_markers = BTreeSet::new();
        for _ in 0..MAX_ENTRIES_PER_PASS {
            let Some(entry) = cursor.next(&root).map_err(unavailable)? else {
                break;
            };
            let (name, version, finished) = match entry {
                Entry::Directory(name) => (name, None, false),
                Entry::Material(name, version) => (name, Some(version), false),
                Entry::FinishedDirectory(name) => (name, None, true),
            };
            let id = match deleted_id(&name) {
                Some(id)
                    if !index.certificates.contains_key(id)
                        && !durable
                            .as_ref()
                            .is_some_and(|index| index.certificates.contains_key(id)) =>
                {
                    id
                }
                Some(_) => continue,
                None if is_canonical_uuid_v7(&name) => &name,
                None => continue,
            };
            if leases.contains(id) {
                cursor.skip_directory();
                continue;
            }
            match root.owned_entry_metadata(&name) {
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    cursor.skip_directory();
                    continue;
                }
                Err(error) => return Err(unavailable(error)),
            }
            let certificate = root.open_dir(&name).map_err(unavailable)?;
            if manifests_checked.insert(name.clone())
                && let Some(candidate) = candidate_manifest(&certificate, id)?
            {
                let pending = index.pending_candidates.contains_key(id)
                    || durable
                        .as_ref()
                        .is_some_and(|index| index.pending_candidates.contains_key(id));
                if !candidate.retired || pending {
                    for material in [candidate.staged.material_id, candidate.base_material_id]
                        .into_iter()
                        .flatten()
                    {
                        protected
                            .entry(id.to_owned())
                            .or_default()
                            .insert(material.to_ascii_lowercase());
                    }
                } else if !index.certificates.contains_key(id)
                    && !durable
                        .as_ref()
                        .is_some_and(|index| index.certificates.contains_key(id))
                {
                    retired_markers.insert(name.clone());
                }
            }
            let reclaim_marker = retired_markers.contains(&name);
            if finished {
                if !index.certificates.contains_key(id)
                    && !protected.contains_key(id)
                    && empty_certificate_tree(&certificate, reclaim_marker)?
                {
                    remove_owned_root(&root, &name)?;
                    remaining -= 1;
                } else if reclaim_marker {
                    certificate
                        .remove_file(super::CANDIDATE_MANIFEST_FILE)
                        .map_err(unavailable)?;
                    remaining -= 1;
                }
                if remaining == 0 {
                    break;
                }
                continue;
            }
            match certificate.owned_entry_metadata("versions") {
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(unavailable(error)),
            }
            let versions = match certificate.open_dir("versions") {
                Ok(versions) => versions,
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    cursor.skip_directory();
                    if (deleted_id(&name).is_some() || reclaim_marker)
                        && !protected.contains_key(id)
                        && empty_certificate_tree(&certificate, reclaim_marker)?
                    {
                        remove_owned_root(&root, &name)?;
                        remaining -= 1;
                    } else if reclaim_marker {
                        certificate
                            .remove_file(super::CANDIDATE_MANIFEST_FILE)
                            .map_err(unavailable)?;
                        remaining -= 1;
                    }
                    if remaining == 0 {
                        break;
                    }
                    continue;
                }
                Err(error) => return Err(unavailable(error)),
            };
            if let Some(version) = version {
                if protected
                    .get(id)
                    .is_some_and(|set| set.contains(&version.to_ascii_lowercase()))
                {
                    continue;
                }
                if !material_id(&version) && !staging_id(&version, id) {
                    continue;
                }
                match versions.owned_entry_metadata(&version) {
                    Ok(_) => {}
                    Err(error) if error.kind() == ErrorKind::NotFound => continue,
                    Err(error) => return Err(unavailable(error)),
                }
                let directory = versions.open_dir(&version).map_err(unavailable)?;
                if owned_material_tree(&directory)? {
                    versions.remove_dir_tree(&version).map_err(unavailable)?;
                    remaining -= 1;
                    if remaining == 0 {
                        return Ok(());
                    }
                }
            } else {
                cursor.visit(name, &versions).map_err(unavailable)?;
            }
            if remaining == 0 {
                return Ok(());
            }
        }
        Ok(())
    }
}

fn remove_owned_root(root: &SafeDir, name: &str) -> Result<(), CertificateError> {
    let tombstone = if deleted_id(name).is_some() {
        name.to_owned()
    } else {
        let tombstone = format!(
            ".deleted-{name}-{}-{}",
            std::process::id(),
            super::OffsetDateTime::now_utc().unix_timestamp_nanos()
        );
        root.rename_dir(name, &tombstone).map_err(unavailable)?;
        tombstone
    };
    root.remove_dir_tree(&tombstone).map_err(unavailable)
}

fn candidate_manifest(
    directory: &SafeDir,
    id: &str,
) -> Result<Option<StoredCertificateCandidate>, CertificateError> {
    match directory.owned_entry_metadata(super::CANDIDATE_MANIFEST_FILE) {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(unavailable(error)),
    }
    let bytes = read_regular_private_file(
        directory,
        super::CANDIDATE_MANIFEST_FILE,
        super::MAX_CANDIDATE_MANIFEST_BYTES,
    )?
    .ok_or(CertificateError::StoreUnavailable)?;
    let candidate =
        serde_json::from_slice(&bytes).map_err(|_| CertificateError::StoreUnavailable)?;
    if !candidate_is_valid(id, &candidate) {
        return Err(CertificateError::StoreUnavailable);
    }
    Ok(Some(candidate))
}

fn empty_certificate_tree(
    directory: &SafeDir,
    terminal_marker: bool,
) -> Result<bool, CertificateError> {
    for name in names(directory)? {
        let name = name?;
        if terminal_marker && name == super::CANDIDATE_MANIFEST_FILE {
            directory.file_path(&name).map_err(unavailable)?;
            continue;
        }
        if name != "versions" {
            return Ok(false);
        }
        let versions = directory.open_dir(&name).map_err(unavailable)?;
        if std::fs::read_dir(versions.path())
            .map_err(unavailable)?
            .next()
            .transpose()
            .map_err(unavailable)?
            .is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn protect_index(index: &CertificateIndex, protected: &mut BTreeMap<String, BTreeSet<String>>) {
    for (id, entry) in &index.certificates {
        let materials = protected.entry(id.clone()).or_default();
        materials.extend(
            entry
                .material_id
                .iter()
                .map(|material| material.to_ascii_lowercase()),
        );
        if let Some(candidate) = index.pending_candidates.get(id) {
            materials.extend(
                candidate
                    .staged
                    .material_id
                    .iter()
                    .map(|material| material.to_ascii_lowercase()),
            );
            materials.extend(
                candidate
                    .base_material_id
                    .iter()
                    .map(|material| material.to_ascii_lowercase()),
            );
        }
    }
}

pub(super) fn restore_interrupted_deletions(
    root: &SafeDir,
    index: &CertificateIndex,
) -> Result<(), CertificateError> {
    let mut missing = BTreeSet::new();
    for id in index.certificates.keys() {
        match root.owned_entry_metadata(id) {
            Ok(_) => {
                root.open_dir(id).map_err(unavailable)?;
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                missing.insert(id.as_str());
            }
            Err(error) => return Err(unavailable(error)),
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    let mut tombstones = BTreeMap::new();
    for name in names(root)? {
        let name = name?;
        let Some(id) = deleted_id(&name) else {
            continue;
        };
        if !missing.contains(id) {
            continue;
        }
        if tombstones.insert(id.to_owned(), name).is_some() {
            return Err(CertificateError::StoreUnavailable);
        }
    }
    for (id, name) in tombstones {
        match root.open_dir(&id) {
            Ok(_) => return Err(CertificateError::StoreUnavailable),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(unavailable(error)),
        }
        let tombstone = root.open_dir(&name).map_err(unavailable)?;
        if let Some(material) = index
            .certificates
            .get(&id)
            .and_then(|entry| entry.material_id.as_ref())
        {
            tombstone
                .owned_entry_metadata("versions")
                .map_err(unavailable)?;
            let versions = tombstone.open_dir("versions").map_err(unavailable)?;
            versions
                .owned_entry_metadata(material)
                .map_err(unavailable)?;
            let version = versions.open_dir(material).map_err(unavailable)?;
            for file in ["fullchain.pem", "private-key.pem"] {
                version.owned_entry_metadata(file).map_err(unavailable)?;
                version.file_path(file).map_err(unavailable)?;
            }
        }
        root.rename_dir(&name, &id).map_err(unavailable)?;
    }
    Ok(())
}

fn names(
    directory: &SafeDir,
) -> Result<impl Iterator<Item = Result<String, CertificateError>> + '_, CertificateError> {
    Ok(std::fs::read_dir(directory.path())
        .map_err(unavailable)?
        .map(|entry| {
            let name = entry
                .map_err(unavailable)?
                .file_name()
                .into_string()
                .map_err(|_| CertificateError::StoreUnavailable)?;
            directory.owned_entry_metadata(&name).map_err(unavailable)?;
            Ok(name)
        }))
}

fn owned_material_tree(directory: &SafeDir) -> Result<bool, CertificateError> {
    for (count, name) in names(directory)?.enumerate() {
        if count >= 4 {
            return Ok(false);
        }
        let name = name?;
        if !["fullchain.pem", "private-key.pem"].contains(&name.as_str())
            && !["fullchain.pem", "private-key.pem"]
                .iter()
                .any(|base| atomic_temporary(&name, base))
        {
            return Ok(false);
        }
        directory.file_path(&name).map_err(unavailable)?;
    }
    Ok(true)
}

fn material_id(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn staging_id(name: &str, id: &str) -> bool {
    let Some(rest) = name.strip_prefix(&format!(".staging-{id}-")) else {
        return false;
    };
    let Some((material, suffix)) = rest.split_once('-') else {
        return false;
    };
    material_id(material) && process_timestamp(suffix)
}

fn deleted_id(name: &str) -> Option<&str> {
    let rest = name.strip_prefix(".deleted-")?;
    let id = rest.get(..36)?;
    (is_canonical_uuid_v7(id) && rest.get(36..37)? == "-" && process_timestamp(rest.get(37..)?))
        .then_some(id)
}

fn process_timestamp(value: &str) -> bool {
    value
        .split_once('-')
        .is_some_and(|(pid, timestamp)| decimal_pair(pid, timestamp))
}

fn atomic_temporary(name: &str, base: &str) -> bool {
    name.strip_prefix(&format!(".{base}."))
        .and_then(|value| value.strip_suffix(".tmp"))
        .and_then(|value| value.split_once('.'))
        .is_some_and(|(pid, timestamp)| decimal_pair(pid, timestamp))
}

fn decimal_pair(pid: &str, timestamp: &str) -> bool {
    pid.bytes().all(|byte| byte.is_ascii_digit())
        && pid.parse::<u32>().is_ok()
        && timestamp.bytes().all(|byte| byte.is_ascii_digit())
        && timestamp.parse::<u128>().is_ok()
}

fn unavailable(_: std::io::Error) -> CertificateError {
    CertificateError::StoreUnavailable
}
