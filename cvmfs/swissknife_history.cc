/**
 * This file is part of the CernVM File System
 */

#include "cvmfs_config.h"
#include "swissknife_history.h"

#include <ctime>
#include <cassert>

#include "hash.h"
#include "util.h"
#include "manifest_fetch.h"
#include "download.h"
#include "signature.h"
#include "catalog_rw.h"
#include "upload.h"

using namespace swissknife;  // NOLINT


const std::string CommandTag::kHeadTag         = "trunk";
const std::string CommandTag::kPreviousHeadTag = "trunk-previous";

const std::string CommandTag::kHeadTagDescription         = "current HEAD";
const std::string CommandTag::kPreviousHeadTagDescription = "default undo target";


void CommandTag::InsertCommonParameters(ParameterList &r) {
  r.push_back(Parameter::Mandatory('w', "repository directory / url"));
  r.push_back(Parameter::Mandatory('t', "temporary scratch directory"));
  r.push_back(Parameter::Optional ('p', "public key of the repository"));
  r.push_back(Parameter::Optional ('z', "trusted certificates"));
  r.push_back(Parameter::Optional ('f', "fully qualified repository name"));
  r.push_back(Parameter::Optional ('r', "spooler definition string"));
  r.push_back(Parameter::Optional ('m', "(unsigned) manifest file to edit"));
  r.push_back(Parameter::Optional ('b', "mounted repository base hash"));
  r.push_back(Parameter::Optional ('e', "hash algorithm to use (default SHA1)"));
}


CommandTag::Environment* CommandTag::InitializeEnvironment(
                                              const ArgumentList  &args,
                                              const bool           read_write) {
  const std::string       repository_url  = MakeCanonicalPath(
                                                       *args.find('w')->second);
  const std::string       tmp_path        = MakeCanonicalPath(
                                                       *args.find('t')->second);
  const std::string       spl_definition  = (args.find('r') == args.end())
                                              ? ""
                                              : MakeCanonicalPath(
                                                       *args.find('r')->second);
  const std::string       manifest_path   = (args.find('m') == args.end())
                                              ? ""
                                              : MakeCanonicalPath(
                                                       *args.find('m')->second);
  const shash::Algorithms hash_algo       = (args.find('e') == args.end())
                                              ? shash::kSha1
                                              : shash::ParseHashAlgorithm(
                                                       *args.find('e')->second);
  const std::string       pubkey_path     = (args.find('p') == args.end())
                                              ? ""
                                              : MakeCanonicalPath(
                                                       *args.find('p')->second);
  const std::string       trusted_certs   = (args.find('z') == args.end())
                                              ? ""
                                              : MakeCanonicalPath(
                                                       *args.find('z')->second);
  const shash::Any        base_hash       = (args.find('b') == args.end())
                                              ? shash::Any()
                                              : shash::MkFromHexPtr(
                                                    shash::HexPtr(
                                                      *args.find('b')->second));
  const std::string       repo_name       = (args.find('f') == args.end())
                                              ? ""
                                              : *args.find('f')->second;

  // do some sanity checks
  if (hash_algo == shash::kAny) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to parse hash algorithm to use");
    return NULL;
  }

  if (read_write && spl_definition.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "no upstream storage provided (-r)");
    return NULL;
  }

  if (read_write && manifest_path.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "no (unsigned) manifest provided (-m)");
    return NULL;
  }

  if (! read_write && pubkey_path.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "no public key provided (-p)");
    return NULL;
  }

  if (! read_write && repo_name.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "no repository name provided (-f)");
    return NULL;
  }

  // create new environment
  // Note: We use this encapsulation because we cannot be sure that the Command
  //       object gets deleted properly. With the Environment object at hand
  //       we have full control and can make heavy and safe use of RAII
  UniquePtr<Environment> env(new Environment(repository_url,
                                             tmp_path));
  env->manifest_path.Set(manifest_path);
  env->history_path.Set(CreateTempPath(tmp_path + "/history", 0600));

  // initialize the (swissknife global) download manager
  g_download_manager->Init(1, true);

  // open the (yet unsigned) manifest file if it is there, otherwise load the
  // latest manifest from the server
  env->manifest = (FileExists(env->manifest_path.path()))
                    ? manifest::Manifest::LoadFile(env->manifest_path.path())
                    : FetchManifest(env->repository_url,
                                    repo_name,
                                    pubkey_path,
                                    trusted_certs,
                                    base_hash);

  if (! env->manifest) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load manifest file");
    return NULL;
  }

  // figure out the hash of the history from the previous revision if needed
  if (read_write && env->manifest->history().IsNull() && ! base_hash.IsNull()) {
    env->previous_manifest = FetchManifest(env->repository_url,
                                           repo_name,
                                           pubkey_path,
                                           trusted_certs,
                                           base_hash);
    if (! env->previous_manifest) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to load previous manifest");
      return NULL;
    }

    LogCvmfs(kLogCvmfs, kLogDebug, "using history database '%s' from previous "
                                   "manifest (%s) as basis",
             env->previous_manifest->history().ToString().c_str(),
             env->previous_manifest->repository_name().c_str());
    env->manifest->set_history(env->previous_manifest->history());
    env->manifest->set_repository_name(env->previous_manifest->repository_name());
  }

  // download the history database referenced in the manifest
  env->history = GetHistory(env->manifest.weak_ref(),
                            env->repository_url,
                            env->history_path.path(),
                            read_write);
  if (! env->history) {
    return NULL;
  }

  // if the using Command is expected to change the history database, we need
  // to initialize the upload spooler for potential later history upload
  if (read_write) {
    const bool use_file_chunking = false;
    const upload::SpoolerDefinition sd(spl_definition,
                                       hash_algo,
                                       use_file_chunking);
    env->spooler = upload::Spooler::Construct(sd);
    if (! env->spooler) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to initialize upload spooler");
      return NULL;
    }
  }

  // return the pointer of the Environment (passing the ownership along)
  return env.Release();
}


bool CommandTag::CloseAndPublishHistory(Environment *env) {
  assert (env->spooler.IsValid());

  // set the previous revision pointer of the history database
  env->history->SetPreviousRevision(env->manifest->history());

  // close the history database
  history::History *weak_history = env->history.Release();
  delete weak_history;

  // compress and upload the new history database
  Future<shash::Any> history_hash;
  upload::Spooler::callback_t* callback =
    env->spooler->RegisterListener(&CommandTag::UploadClosure,
                                    this,
                                   &history_hash);
  env->spooler->ProcessHistory(env->history_path.path());
  env->spooler->WaitForUpload();
  const shash::Any new_history_hash = history_hash.Get();
  env->spooler->UnregisterListener(callback);

  // retrieve the (async) uploader result
  if (new_history_hash.IsNull()) {
    return false;
  }

  // update the (yet unsigned) manifest file
  env->manifest->set_history(new_history_hash);
  if (! env->manifest->Export(env->manifest_path.path())) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to export the new manifest '%s'",
             env->manifest_path.path().c_str());
    return false;
  }

  // disable the unlink guard in order to keep the newly exported manifest file
  env->manifest_path.Disable();
  LogCvmfs(kLogCvmfs, kLogVerboseMsg, "exported manifest (%d) with new "
                                      "history '%s'",
           env->manifest->revision(), new_history_hash.ToString().c_str());

  // all done
  return true;
}


bool CommandTag::UploadCatalogAndUpdateManifest(
                                           CommandTag::Environment   *env,
                                           catalog::WritableCatalog  *catalog) {
  assert (env->spooler.IsValid());

  // gather information about catalog to be uploaded and update manifest
  UniquePtr<catalog::WritableCatalog> wr_catalog(catalog);
  const std::string catalog_path  = wr_catalog->database_path();
  env->manifest->set_ttl              (wr_catalog->GetTTL());
  env->manifest->set_revision         (wr_catalog->GetRevision());
  env->manifest->set_publish_timestamp(wr_catalog->GetLastModified());

  // close the catalog
  catalog::WritableCatalog *weak_catalog = wr_catalog.Release();
  delete weak_catalog;

  // upload the catalog
  Future<shash::Any> catalog_hash;
  upload::Spooler::callback_t* callback =
    env->spooler->RegisterListener(&CommandTag::UploadClosure,
                                    this,
                                   &catalog_hash);
  env->spooler->ProcessCatalog(catalog_path);
  env->spooler->WaitForUpload();
  const shash::Any new_catalog_hash = catalog_hash.Get();
  env->spooler->UnregisterListener(callback);

  // check if the upload succeeded
  if (new_catalog_hash.IsNull()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to upload catalog '%s'",
             catalog_path.c_str());
    return false;
  }

  // update the catalog size and hash in the manifest
  const size_t catalog_size = GetFileSize(catalog_path);
  env->manifest->set_catalog_size(catalog_size);
  env->manifest->set_catalog_hash(new_catalog_hash);

  LogCvmfs(kLogCvmfs, kLogVerboseMsg, "uploaded new catalog (%d bytes) '%s'",
           catalog_size, new_catalog_hash.ToString().c_str());

  return true;
}


void CommandTag::UploadClosure(const upload::SpoolerResult  &result,
                                     Future<shash::Any>     *hash) {
  assert (! result.IsChunked());
  if (result.return_code != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to upload history database (%d)",
             result.return_code);
    hash->Set(shash::Any());
  } else {
    hash->Set(result.content_hash);
  }
}


bool CommandTag::UpdateUndoTags(
                          Environment                  *env,
                          const history::History::Tag  &current_head_template,
                          const bool                    undo_rollback) {
  assert (env->history.IsValid());

  history::History::Tag current_head;
  history::History::Tag current_old_head;

  // remove previous HEAD tag
  if (! env->history->Remove(CommandTag::kPreviousHeadTag)) {
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "didn't find a previous HEAD tag");
  }

  // check if we have a current HEAD tag that needs to renamed to previous HEAD
  if (env->history->GetByName(CommandTag::kHeadTag, &current_head)) {
    // remove current HEAD tag
    if (! env->history->Remove(CommandTag::kHeadTag)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to remove current HEAD tag");
      return false;
    }

    // set previous HEAD tag where current HEAD used to be
    if (! undo_rollback) {
      current_old_head             = current_head;
      current_old_head.name        = CommandTag::kPreviousHeadTag;
      current_old_head.channel     = history::History::kChannelTrunk;
      current_old_head.description = CommandTag::kPreviousHeadTagDescription;
      if (! env->history->Insert(current_old_head)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to set previous HEAD tag");
        return false;
      }
    }
  }

  // set the current HEAD to the catalog provided by the template HEAD
  current_head             = current_head_template;
  current_head.name        = CommandTag::kHeadTag;
  current_head.channel     = history::History::kChannelTrunk;
  current_head.description = CommandTag::kHeadTagDescription;
  if (! env->history->Insert(current_head)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to set new current HEAD");
    return false;
  }

  return true;
}


manifest::Manifest* CommandTag::FetchManifest(
                                           const std::string &repository_url,
                                           const std::string &repository_name,
                                           const std::string &pubkey_path,
                                           const std::string &trusted_certs,
                                           const shash::Any  &base_hash) const {
  manifest::ManifestEnsemble *manifest_ensemble = new manifest::ManifestEnsemble;
  UniquePtr<manifest::Manifest> manifest;

  // initialize the (global) signature manager
  g_signature_manager->Init();
  if (! g_signature_manager->LoadPublicRsaKeys(pubkey_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load public repository key '%s'",
             pubkey_path.c_str());
    return NULL;
  }

  if (! trusted_certs.empty()) {
    if (! g_signature_manager->LoadTrustedCaCrl(trusted_certs)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to load trusted certificates");
      return NULL;
    }
  }

  // fetch (and verify) the manifest
  manifest::Failures retval;
  retval = manifest::Fetch(repository_url, repository_name, 0, NULL,
                           g_signature_manager, g_download_manager,
                           manifest_ensemble);

  if (retval != manifest::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to fetch repository manifest "
                                    "(%d - %s)",
             retval, manifest::Code2Ascii(retval));
    delete manifest_ensemble;
    return NULL;
  } else {
    // ManifestEnsemble stays around! This is a memory leak, but otherwise
    // the destructor of ManifestEnsemble would free the wrapped manifest
    // object, but I want to return it.
    // Sorry for that...
    //
    // TODO: Revise the manifest fetching.
    manifest = manifest_ensemble->manifest;
  }

  // check if manifest fetching was successful
  if (! manifest) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest");
    return NULL;
  }

  // check the provided base hash of the repository if provided
  if (! base_hash.IsNull() && manifest->catalog_hash() != base_hash) {
    LogCvmfs(kLogCvmfs, kLogStderr, "base hash does not match manifest "
                                    "(found: %s expected: %s)",
             manifest->catalog_hash().ToString().c_str(),
             base_hash.ToString().c_str());
    return NULL;
  }

  // return the fetched manifest (releasing pointer ownership)
  return manifest.Release();
}


bool CommandTag::FetchObject(const std::string  &repository_url,
                              const shash::Any   &object_hash,
                              const std::string  &hash_suffix,
                              const std::string   destination_path) const {
  assert (! object_hash.IsNull());

  download::Failures dl_retval;
  const std::string url =
    repository_url + "/data" + object_hash.MakePath(1, 2) + hash_suffix;

  download::JobInfo download_object(&url, true, false, &destination_path,
                                    &object_hash);
  dl_retval = g_download_manager->Fetch(&download_object);

  if (dl_retval != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to download object '%s' with "
                                    "suffix '%s' (%d - %s)",
             object_hash.ToString().c_str(), hash_suffix.c_str(),
             dl_retval, download::Code2Ascii(dl_retval));
    return false;
  }

  return true;
}


history::History* CommandTag::GetHistory(
                                const manifest::Manifest  *manifest,
                                const std::string         &repository_url,
                                const std::string         &history_path,
                                const bool                 read_write) const {
  const shash::Any history_hash = manifest->history();
  history::History *history;

  if (history_hash.IsNull()) {
    history = history::History::Create(history_path,
                                       manifest->repository_name());
    if (NULL == history) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to create history database");
      return NULL;
    }
  } else {
    if (! FetchObject(repository_url, history_hash, "H", history_path)) {
      return NULL;
    }

    history = (read_write) ? history::History::OpenWritable(history_path)
                           : history::History::Open(history_path);
    if (NULL == history) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to open history database (%s)",
               history_path.c_str());
      unlink(history_path.c_str());
      return NULL;
    }

    assert (history->fqrn() == manifest->repository_name());
  }

  return history;
}


catalog::Catalog* CommandTag::GetCatalog(
                                       const std::string  &repository_url,
                                       const shash::Any   &catalog_hash,
                                       const std::string   catalog_path,
                                       const bool          read_write) const {
  if (! FetchObject(repository_url, catalog_hash, "C", catalog_path)) {
    return NULL;
  }

  const std::string catalog_root_path = "";
  return (read_write)
    ? catalog::WritableCatalog::AttachFreely(catalog_root_path,
                                             catalog_path,
                                             catalog_hash)
    : catalog::Catalog::AttachFreely(catalog_root_path,
                                     catalog_path,
                                     catalog_hash);
}


void CommandTag::PrintTagMachineReadable(const history::History::Tag &tag) const
{
   LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %d %d %d %s %s",
             tag.name.c_str(),
             tag.root_hash.ToString().c_str(),
             StringifyInt(tag.size).c_str(),
             StringifyInt(tag.revision).c_str(),
             StringifyInt(tag.timestamp).c_str(),
             tag.GetChannelName(),
             tag.description.c_str());
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


ParameterList CommandCreateTag::GetParams() {
  ParameterList r;
  InsertCommonParameters(r);

  r.push_back(Parameter::Mandatory('a', "name of the new tag"));
  r.push_back(Parameter::Optional ('d', "description of the tag"));
  r.push_back(Parameter::Optional ('h', "root hash of the new tag"));
  r.push_back(Parameter::Optional ('c', "channel of the new tag"));
  r.push_back(Parameter::Switch   ('x', "maintain undo tags"));
  return r;
}


int CommandCreateTag::Main(const ArgumentList &args) {
  typedef history::History::UpdateChannel TagChannel;
  const std::string tag_name         = *args.find('a')->second;
  const std::string tag_description  = (args.find('d') != args.end())
                                         ? *args.find('d')->second
                                         : "";
  const TagChannel  tag_channel      = (args.find('c') != args.end())
                                         ? static_cast<TagChannel>(
                                             String2Uint64(
                                               *args.find('c')->second))
                                         : history::History::kChannelTrunk;
  const bool        undo_tags        = (args.find('x') != args.end());
  const std::string root_hash_string = (args.find('h') != args.end())
                                         ? *args.find('h')->second
                                         : "";

  if (tag_name.find(" ") != std::string::npos) {
    LogCvmfs(kLogCvmfs, kLogStderr, "tag names must not contain spaces");
    return 1;
  }

  // initialize the Environment (taking ownership)
  const bool history_read_write = true;
  UniquePtr<Environment> env(InitializeEnvironment(args, history_read_write));
  if (! env) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init environment");
    return 1;
  }

  // set the root hash to be tagged to the current HEAD if no other hash was
  // given by the user
  shash::Any root_hash;
  if (root_hash_string.empty()) {
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "no catalog hash provided, using hash"
                                        "of current HEAD catalog (%s)",
             env->manifest->catalog_hash().ToString().c_str());
    root_hash = env->manifest->catalog_hash();
  } else {
    root_hash = shash::MkFromHexPtr(shash::HexPtr(root_hash_string));
    if (root_hash.IsNull()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to read provided catalog "
                                      "hash '%s'",
               root_hash_string.c_str());
      return 1;
    }
  }

  // open the catalog to be tagged (to check for existance and for meta info)
  const UnlinkGuard catalog_path(CreateTempPath(env->tmp_path + "/catalog",
                                                0600));
  const bool catalog_read_write = false;
  const UniquePtr<catalog::Catalog> catalog(GetCatalog(env->repository_url,
                                                       root_hash,
                                                       catalog_path.path(),
                                                       catalog_read_write));
  if (! catalog) {
    LogCvmfs(kLogCvmfs, kLogStderr, "catalog with hash '%s' does not exist",
             root_hash.ToString().c_str());
    return 1;
  }

  // check if the tag to be created exists (and move it perhaps)
  bool move_tag = false;
  if (env->history->Exists(tag_name)) {
    if (root_hash_string.empty()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "a tag with the name '%s' already exists. "
                                      "Do you want to move it? (-h <root hash>)",
               tag_name.c_str());
      return 1;
    }

    move_tag = true;
  }

  // get the already existent tag (in the moving case)
  history::History::Tag new_tag;
  shash::Any            old_hash;
  if (move_tag && ! env->history->GetByName(tag_name, &new_tag)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to retrieve tag '%s' for moving",
             tag_name.c_str());
    return 1;
  } else {
    old_hash = new_tag.root_hash;
  }

  // set up the tag to be inserted
  new_tag.name        = tag_name;
  new_tag.root_hash   = root_hash;
  new_tag.size        = GetFileSize(catalog_path.path());
  new_tag.revision    = catalog->GetRevision();
  new_tag.timestamp   = catalog->GetLastModified();
  new_tag.channel     = tag_channel;
  new_tag.description = (! tag_description.empty()) ? tag_description : "";

  // insert the new tag into the history database
  if (move_tag) {
    assert (! old_hash.IsNull());
    LogCvmfs(kLogCvmfs, kLogStdout, "moving tag '%s' from '%s' to '%s'",
             tag_name.c_str(),
             old_hash.ToString().c_str(),
             root_hash.ToString().c_str());

    if (! env->history->Remove(tag_name)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "removing old tag '%s' before move failed",
               tag_name.c_str());
      return 1;
    }
  }

  if (! env->history->Insert(new_tag)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to insert new tag");
    return 1;
  }

  // handle undo tags ('trunk' and 'trunk-previous') if necessary
  if (undo_tags && ! UpdateUndoTags(env.weak_ref(), new_tag)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to update magic undo tags");
    return 1;
  }

  // finalize processing and upload new history database
  if (! CloseAndPublishHistory(env.weak_ref())) {
    return 1;
  }

  return 0;
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


ParameterList CommandRemoveTag::GetParams() {
  ParameterList r;
  InsertCommonParameters(r);

  r.push_back(Parameter::Mandatory('d', "space separated tags to be deleted"));
  return r;
}


int CommandRemoveTag::Main(const ArgumentList &args) {
  typedef std::vector<std::string> TagNames;
  const std::string tags_to_delete = *args.find('d')->second;

  const TagNames condemned_tags = SplitString(tags_to_delete, ' ');
  LogCvmfs(kLogCvmfs, kLogDebug, "proceeding to delete %d tags",
           condemned_tags.size());

  // initialize the Environment (taking ownership)
  const bool history_read_write = true;
  UniquePtr<Environment> env(InitializeEnvironment(args, history_read_write));
  if (! env) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init environment");
    return 1;
  }

  // check if the tags to be deleted exist
        TagNames::const_iterator i    = condemned_tags.begin();
  const TagNames::const_iterator iend = condemned_tags.end();
  bool all_exist = true;
  for (; i != iend; ++i) {
    if (! env->history->Exists(*i)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "tag '%s' does not exist",
               i->c_str());
      all_exist = false;
    }
  }
  if (! all_exist) {
    return 1;
  }

  // delete the tags from the tag database
  i = condemned_tags.begin();
  env->history->BeginTransaction();
  for (; i != iend; ++i) {
    if (! env->history->Remove(*i)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to remove tag '%s' from history",
               i->c_str());
      return 1;
    }
  }
  env->history->CommitTransaction();

  // finalize processing and upload new history database
  if (! CloseAndPublishHistory(env.weak_ref())) {
    return 1;
  }

  return 0;
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


ParameterList CommandListTags::GetParams() {
  ParameterList r;
  InsertCommonParameters(r);
  r.push_back(Parameter::Switch('x', "machine readable output"));
  return r;
}

std::string CommandListTags::AddPadding(const std::string  &str,
                                        const size_t        padding,
                                        const bool          align_right,
                                        const std::string  &fill_char) const {
  assert (str.size() <= padding);
  std::string result(str);
  result.reserve(padding);
  const size_t pos = (align_right) ? 0 : str.size();
  const size_t padding_width = padding - str.size();
  for (size_t i = 0; i < padding_width; ++i) result.insert(pos, fill_char);
  return result;
}


void CommandListTags::PrintHumanReadableList(
                                   const CommandListTags::TagList &tags) const {
  // go through the list of tags and figure out the column widths
  const std::string name_label = "Name";
  const std::string rev_label  = "Revision";
  const std::string chan_label = "Channel";
  const std::string time_label = "Timestamp";
  const std::string desc_label = "Description";

  // figure out the maximal lengths of the fields in the lists
        TagList::const_iterator i    = tags.begin();
  const TagList::const_iterator iend = tags.end();
  size_t max_name_len = name_label.size();
  size_t max_rev_len  = rev_label.size();
  size_t max_chan_len = chan_label.size();
  size_t max_time_len = desc_label.size();
  for (; i != iend; ++i) {
    max_name_len = std::max(max_name_len, i->name.size());
    max_rev_len  = std::max(max_rev_len,  StringifyInt(i->revision).size());
    max_chan_len = std::max(max_chan_len, strlen(i->GetChannelName()));
    max_time_len = std::max(max_time_len, StringifyTime(i->timestamp, true).size());
  }

  // print the list header
  LogCvmfs(kLogCvmfs, kLogStdout, "%s \u2502 %s \u2502 %s \u2502 %s \u2502 %s",
           AddPadding(name_label, max_name_len).c_str(),
           AddPadding(rev_label,  max_rev_len).c_str(),
           AddPadding(chan_label, max_chan_len).c_str(),
           AddPadding(time_label, max_time_len).c_str(),
           desc_label.c_str());
  LogCvmfs(kLogCvmfs, kLogStdout, "%s\u2500\u253C\u2500%s\u2500\u253C\u2500%s"
                                  "\u2500\u253C\u2500%s\u2500\u253C\u2500%s",
           AddPadding("", max_name_len,          false, "\u2500").c_str(),
           AddPadding("", max_rev_len,           false, "\u2500").c_str(),
           AddPadding("", max_chan_len,          false, "\u2500").c_str(),
           AddPadding("", max_time_len,          false, "\u2500").c_str(),
           AddPadding("", desc_label.size() + 1, false, "\u2500").c_str());

  // print the rows of the list
  i = tags.begin();
  for (; i != iend; ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s \u2502 %s \u2502 %s \u2502 %s \u2502 %s",
             AddPadding(i->name,                           max_name_len).c_str(),
             AddPadding(StringifyInt(i->revision),         max_rev_len, true).c_str(),
             AddPadding(i->GetChannelName(),               max_chan_len).c_str(),
             AddPadding(StringifyTime(i->timestamp, true), max_time_len).c_str(),
             i->description.c_str());
  }

  // print the list footer
  LogCvmfs(kLogCvmfs, kLogStdout, "%s\u2500\u2534\u2500%s\u2500\u2534\u2500%s"
                                  "\u2500\u2534\u2500%s\u2500\u2534\u2500%s",
           AddPadding("", max_name_len,          false, "\u2500").c_str(),
           AddPadding("", max_rev_len,           false, "\u2500").c_str(),
           AddPadding("", max_chan_len,          false, "\u2500").c_str(),
           AddPadding("", max_time_len,          false, "\u2500").c_str(),
           AddPadding("", desc_label.size() + 1, false, "\u2500").c_str());

  // print the number of tags listed
  LogCvmfs(kLogCvmfs, kLogStdout, "listing contains %d tags", tags.size());
}


void CommandListTags::PrintMachineReadableList(const TagList &tags) const {
        TagList::const_iterator i    = tags.begin();
  const TagList::const_iterator iend = tags.end();
  for (; i != iend; ++i) {
    PrintTagMachineReadable(*i);
  }
}


int CommandListTags::Main(const ArgumentList &args) {
  const bool machine_readable = (args.find('x') != args.end());

  // initialize the Environment (taking ownership)
  const bool history_read_write = false;
  UniquePtr<Environment> env(InitializeEnvironment(args, history_read_write));
  if (! env) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init environment");
    return 1;
  }

  // obtain a full list of all tags
  TagList tags;
  if (! env->history->List(&tags)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to list tags in history database");
    return 1;
  }

  if (machine_readable) {
    PrintMachineReadableList(tags);
  } else {
    PrintHumanReadableList(tags);
  }

  return 0;
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


ParameterList CommandInfoTag::GetParams() {
  ParameterList r;
  InsertCommonParameters(r);

  r.push_back(Parameter::Mandatory('n', "name of the tag to be inspected"));
  return r;
}


std::string CommandInfoTag::HumanReadableFilesize(const size_t filesize) const {
  const size_t kiB = 1024;
  const size_t MiB = kiB * 1024;
  const size_t GiB = MiB * 1024;

  if (filesize > GiB) {
    return StringifyDouble((double)filesize / GiB) + " GiB";
  } else if (filesize > MiB) {
    return StringifyDouble((double)filesize / MiB) + " MiB";
  } else if (filesize > kiB) {
    return StringifyDouble((double)filesize / kiB) + " kiB";
  } else {
    return StringifyInt(filesize) + " Byte";
  }
}


void CommandInfoTag::PrintHumanReadableInfo(
                                       const history::History::Tag &tag) const {
  LogCvmfs(kLogCvmfs, kLogStdout, "Name:         %s\n"
                                  "Revision:     %s\n"
                                  "Channel:      %s\n"
                                  "Timestamp:    %s\n"
                                  "Root Hash:    %s\n"
                                  "Catalog Size: %s\n"
                                  "%s",
           tag.name.c_str(),
           StringifyInt(tag.revision).c_str(),
           tag.GetChannelName(),
           StringifyTime(tag.timestamp, true /* utc */).c_str(),
           tag.root_hash.ToString().c_str(),
           HumanReadableFilesize(tag.size).c_str(),
           tag.description.c_str());
}


int CommandInfoTag::Main(const ArgumentList &args) {
  const std::string tag_name = *args.find('n')->second;

  // initialize the Environment (taking ownership)
  const bool history_read_write = false;
  UniquePtr<Environment> env(InitializeEnvironment(args, history_read_write));
  if (! env) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init environment");
    return 1;
  }

  history::History::Tag tag;
  const bool found = env->history->GetByName(tag_name, &tag);
  if (! found) {
    LogCvmfs(kLogCvmfs, kLogStderr, "tag '%s' does not exist", tag_name.c_str());
    return 1;
  }

  PrintHumanReadableInfo(tag);

  return 0;

}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


ParameterList CommandRollbackTag::GetParams() {
  ParameterList r;
  InsertCommonParameters(r);

  r.push_back(Parameter::Optional('n', "name of the tag to be republished"));
  return r;
}


int CommandRollbackTag::Main(const ArgumentList &args) {
  const bool        undo_rollback = (args.find('n') == args.end());
  const std::string tag_name      = (! undo_rollback)
                                       ? *args.find('n')->second
                                       : CommandTag::kPreviousHeadTag;

  // initialize the Environment (taking ownership)
  const bool history_read_write = true;
  UniquePtr<Environment> env(InitializeEnvironment(args, history_read_write));
  if (! env) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init environment");
    return 1;
  }

  // find tag to be rolled back to
  history::History::Tag target_tag;
  const bool found = env->history->GetByName(tag_name, &target_tag);
  if (! found) {
    if (undo_rollback) {
      LogCvmfs(kLogCvmfs, kLogStderr, "only one anonymous rollback supported - "
                                      "perhaps you want to provide a tag name?");
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "tag '%s' does not exist",
                                      tag_name.c_str());
    }
    return 1;
  }

  // check if tag is valid to be rolled back to
  const uint64_t current_revision = env->manifest->revision();
  assert (target_tag.revision <= current_revision);
  if (target_tag.revision == current_revision) {
    LogCvmfs(kLogCvmfs, kLogStderr, "not rolling back to current head (%u)",
             current_revision);
    return 1;
  }

  // open the catalog to be rolled back to
  const UnlinkGuard catalog_path(CreateTempPath(env->tmp_path + "/catalog",
                                                0600));
  const bool catalog_read_write = true;
  UniquePtr<catalog::WritableCatalog> catalog(
       dynamic_cast<catalog::WritableCatalog*>(GetCatalog(env->repository_url,
                                                          target_tag.root_hash,
                                                          catalog_path.path(),
                                                          catalog_read_write)));
  if (! catalog) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to open catalog with hash '%s'",
             target_tag.root_hash.ToString().c_str());
    return 1;
  }

  // update the catalog to be republished
  catalog->Transaction();
  catalog->UpdateLastModified();
  catalog->SetRevision(current_revision + 1);
  catalog->SetPreviousRevision(env->manifest->catalog_hash());
  catalog->Commit();

  // Upload catalog (handing over ownership of catalog pointer)
  if (! UploadCatalogAndUpdateManifest(env.weak_ref(), catalog.Release())) {
    LogCvmfs(kLogCvmfs, kLogStderr, "catalog upload failed");
    return 1;
  }

  // update target tag with newly published root catalog information
  history::History::Tag updated_target_tag(target_tag);
  updated_target_tag.root_hash   = env->manifest->catalog_hash();
  updated_target_tag.size        = env->manifest->catalog_size();
  updated_target_tag.revision    = env->manifest->revision();
  updated_target_tag.timestamp   = env->manifest->publish_timestamp();
  if (! env->history->Rollback(updated_target_tag)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to rollback history to '%s'",
                                    updated_target_tag.name.c_str());
    return 1;
  }

  // set the magic undo tags
  if (! UpdateUndoTags(env.weak_ref(), updated_target_tag, undo_rollback)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to update magic undo tags");
    return 1;
  }

  // finalize the history and upload it
  if (! CloseAndPublishHistory(env.weak_ref())) {
    return 1;
  }

  return 0;
}
