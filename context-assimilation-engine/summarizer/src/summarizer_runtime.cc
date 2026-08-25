/*
 * Copyright (c) 2024, Gnosis Research Center, Illinois Institute of Technology
 * All rights reserved.
 *
 * This file is part of IOWarp Core.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <clio_cae/summarizer/summarizer_runtime.h>
#include <clio_cae/summarizer/label_client.h>

#include <algorithm>
#include <cstring>
#include <regex>
#include <string>
#include <string_view>
#include <vector>

// Define ChiMod entry points using the CLIO_TASK_CC macro.
CLIO_TASK_CC(clio::cae::summarizer::Runtime)

namespace clio::cae::summarizer {

namespace {

/**
 * Walk the configured rules and return the first (if any) whose tag_re_
 * matches `tag_name` and blob_re_ matches `blob_name`. std::regex_search
 * (not _match) so ".*\\.txt" matches any name ending in .txt without needing
 * an explicit anchor. An invalid regex in either side disables that rule
 * (logged at kWarning) and matching continues with the next one.
 * @param rules The configured summarization rules, in priority order.
 * @param tag_name Resolved name of the blob's tag ("" if unresolvable).
 * @param blob_name Name of the blob being written.
 * @return The winning rule, or nullptr when nothing matches.
 */
const LabelMatch *FindLabelMatch(const std::vector<LabelMatch> &rules,
                                 const std::string &tag_name,
                                 const std::string &blob_name) {
  for (const auto &rule : rules) {
    try {
      std::regex tag_rx(rule.tag_re_);
      std::regex blob_rx(rule.blob_re_);
      if (std::regex_search(tag_name, tag_rx) &&
          std::regex_search(blob_name, blob_rx)) {
        return &rule;
      }
    } catch (const std::regex_error &e) {
      HLOG(kWarning,
           "FindLabelMatch: invalid regex in rule (tag='{}' blob='{}'): {}",
           rule.tag_re_, rule.blob_re_, e.what());
    }
  }
  return nullptr;
}

/**
 * Split a blob payload into per-request slices that fit the model's context.
 *
 * The Ollama API counts both prompt and generated tokens against num_ctx.
 * Reserve ~25% of context for the prompt template + the response budget; the
 * remaining 75% is available for blob payload. Convert tokens to bytes via a
 * conservative ~3 bytes/token English ratio (binary blobs run closer to
 * 1 byte/token, so this errs on splitting *more*).
 *
 * @param payload The blob bytes to summarize.
 * @param prompt_template The template prepended to each chunk.
 * @param ctx_tokens The rule's context_length. <= 0 disables chunking
 *        entirely — the caller takes Ollama's default 2048 and accepts
 *        whatever truncation it does.
 * @return One view per request; always at least one element.
 */
std::vector<std::string_view> SplitPayload(const std::string &payload,
                                           const std::string &prompt_template,
                                           int ctx_tokens) {
  std::vector<std::string_view> chunks;
  if (ctx_tokens <= 0 || payload.empty()) {
    chunks.emplace_back(payload);
    return chunks;
  }
  size_t budget_tokens = static_cast<size_t>(ctx_tokens) * 3 / 4;
  size_t budget_bytes = budget_tokens * 3;
  if (budget_bytes > prompt_template.size() + 256) {
    budget_bytes -= prompt_template.size();
  }
  if (budget_bytes == 0) budget_bytes = 256;  // sanity floor
  for (size_t off = 0; off < payload.size(); off += budget_bytes) {
    size_t take = std::min(budget_bytes, payload.size() - off);
    chunks.emplace_back(payload.data() + off, take);
  }
  return chunks;
}

/**
 * Run the rule's prompt over every chunk of the payload and concatenate the
 * responses. A failure on any one chunk doesn't abort the whole summary — the
 * chunk is skipped and logged; the caller still gets a partial summary. A
 * production deploy would dispatch each chunk to a dedicated inference worker
 * pool; this runs them inline on the calling worker.
 *
 * @param endpoint Base URL of the Ollama-compatible inference server.
 * @param rule The matched rule (model, context_length, num_predict).
 * @param prompt_template Prompt text prepended to each chunk.
 * @param payload The blob bytes to summarize.
 * @param tag_name Tag name, for log context only.
 * @param blob_name Blob name, for log context only.
 * @return The concatenated summary, or "" when every chunk failed.
 */
std::string GenerateSummary(const std::string &endpoint, const LabelMatch &rule,
                            const std::string &prompt_template,
                            const std::string &payload,
                            const std::string &tag_name,
                            const std::string &blob_name) {
  std::vector<std::string_view> chunks =
      SplitPayload(payload, prompt_template, rule.context_length_);
  std::string summary;
  size_t successful_chunks = 0;
  for (size_t i = 0; i < chunks.size(); ++i) {
    std::string full_prompt = prompt_template;
    full_prompt.append("\n\n");
    full_prompt.append(chunks[i].data(), chunks[i].size());

    std::string chunk_summary;
    bool ok = OllamaGenerate(endpoint, rule.model_, full_prompt,
                             rule.context_length_, rule.num_predict_,
                             chunk_summary);
    if (!ok || chunk_summary.empty()) {
      HLOG(kWarning,
           "Summarizer: chunk {} of {} failed for tag='{}' blob='{}' "
           "model='{}'",
           i + 1, chunks.size(), tag_name, blob_name, rule.model_);
      continue;
    }
    if (!summary.empty()) summary.append("\n\n");
    summary.append(chunk_summary);
    ++successful_chunks;
  }
  if (successful_chunks == 0) summary.clear();
  return summary;
}

}  // namespace

// ---------------------------------------------------------------------------
// Container lifecycle
// ---------------------------------------------------------------------------

clio::run::TaskResume Runtime::Create(clio::run::shared_ptr<CreateTask> &task) {
  CLIO_TASK_BODY_BEGIN
  config_ = task->GetParams();
  // CoreInterposer forwards everything we don't handle to this pool.
  interposer_next_pool_ = config_.next_pool_id_;
  next_client_ = std::make_unique<clio::cte::core::Client>(CorePoolId());
  HLOG(kInfo,
       "Summarizer container created for pool: {} (ID: {}), next_pool_id={}, "
       "label_matches={}, label_endpoint='{}'",
       pool_name_, pool_id_, CorePoolId(), config_.label_matches_.size(),
       config_.label_endpoint_);
  task->SetReturnCode(0);
  CLIO_CO_RETURN;
  CLIO_TASK_BODY_END
}

clio::run::TaskResume Runtime::Destroy(
    clio::run::shared_ptr<DestroyTask> &task) {
  CLIO_TASK_BODY_BEGIN
  {
    std::lock_guard<std::mutex> lock(tag_names_mu_);
    tag_names_.clear();
  }
  HLOG(kInfo, "Summarizer container destroyed for pool: {} (ID: {})",
       pool_name_, pool_id_);
  task->SetReturnCode(0);
  CLIO_CO_RETURN;
  CLIO_TASK_BODY_END
}

// LCOV_EXCL_START: admin Monitor RPC hook — a no-op status handler the unit
// suite never dispatches (there is no monitoring client here).
clio::run::TaskResume Runtime::Monitor(
    clio::run::shared_ptr<MonitorTask> &task) {
  CLIO_TASK_BODY_BEGIN
  task->SetReturnCode(0);
  CLIO_CO_RETURN;
  CLIO_TASK_BODY_END
}
// LCOV_EXCL_STOP

clio::cte::core::Client *Runtime::GetNextClient() {
  // LCOV_EXCL_START: Create() always constructs next_client_ before this
  // container can be handed a task, so the lazy branch never runs. Kept as
  // defensive belt-and-braces (the CAE core does the same for its client).
  if (!next_client_) {
    next_client_ = std::make_unique<clio::cte::core::Client>(CorePoolId());
  }
  // LCOV_EXCL_STOP
  return next_client_.get();
}

// ---------------------------------------------------------------------------
// Summarization
// ---------------------------------------------------------------------------

clio::run::TaskResume Runtime::ResolveTagName(const TagId &tag_id,
                                              std::string *name_out) {
  CLIO_TASK_BODY_BEGIN
  {
    std::lock_guard<std::mutex> lock(tag_names_mu_);
    auto it = tag_names_.find(tag_id);
    if (it != tag_names_.end()) {
      *name_out = it->second;
      CLIO_CO_RETURN;
    }
  }
  {
    // Local, not Broadcast: every chain container composes one-per-node, so
    // the tag either resolves on this node or stays uncached until a path
    // that can see it runs — the same trade the indexer makes.
    auto fut = GetNextClient()->AsyncGetTagName(tag_id,
                                                clio::run::PoolQuery::Local());
    CLIO_CO_AWAIT(fut);
    if (fut->GetReturnCode() == 0 && fut->found_ != 0) {
      *name_out = fut->tag_name_.str();
      std::lock_guard<std::mutex> lock(tag_names_mu_);
      tag_names_[tag_id] = *name_out;
    } else {
      name_out->clear();
    }
  }
  CLIO_CO_RETURN;
  CLIO_TASK_BODY_END
}

clio::run::TaskResume Runtime::StoreSummary(const TagId &tag_id,
                                            const std::string &blob_name,
                                            float score,
                                            const std::string &text) {
  CLIO_TASK_BODY_BEGIN
  {
    std::string summary_blob_name = blob_name + "_label";
    auto *ipc = CLIO_IPC;
    auto buf = ipc->AllocateBuffer(text.size());
    if (buf.IsNull()) {
      HLOG(kWarning, "Summarizer: summary SHM allocation failed");
      CLIO_CO_RETURN;
    }
    std::memcpy(buf.ptr_, text.data(), text.size());
    ctp::ipc::ShmPtr<> shm = buf.shm_.template Cast<void>();
    clio::cte::core::Context ctx;
    // Addressed at the pool BELOW this one, so the write does not re-enter
    // this handler — no summarize-the-summary loop.
    auto fut = GetNextClient()->AsyncPutBlob(
        tag_id, summary_blob_name, 0, static_cast<clio::run::u64>(text.size()),
        shm, score, ctx, 0, clio::run::PoolQuery::Local());
    CLIO_CO_AWAIT(fut);
    ipc->FreeBuffer(buf);
    if (fut->GetReturnCode() != 0) {
      HLOG(kWarning, "Summarizer: failed to store summary blob '{}' (rc={})",
           summary_blob_name, fut->GetReturnCode());
    }
  }
  CLIO_CO_RETURN;
  CLIO_TASK_BODY_END
}

clio::run::TaskResume Runtime::PutBlob(
    clio::run::shared_ptr<PutBlobTask> &task) {
  CLIO_TASK_BODY_BEGIN
  // 1. Forward the original blob down the chain FIRST so the user's write
  //    semantic is preserved regardless of the summarization outcome.
  CLIO_CO_AWAIT(ForwardToCore(clio::cte::core::Method::kPutBlob,
                              task.template Cast<clio::run::Task>()));

  // 2. Gate. Nothing configured, a failed write, or a replica-addressed
  //    write (whose primary already flowed through here) short-circuits.
  //    A summarization failure must never flip the PutBlob return code.
  if (config_.label_matches_.empty() || task->GetReturnCode() != 0 ||
      task->context_.replica_ != 0) {
    CLIO_CO_RETURN;
  }
  {
    // blob_name_ is INOUT (handlers below may compose a suffix), so read it
    // after the forward.
    std::string blob_name = task->blob_name_.str();
    std::string tag_name;
    CLIO_CO_AWAIT(ResolveTagName(task->tag_id_, &tag_name));
    const LabelMatch *rule =
        FindLabelMatch(config_.label_matches_, tag_name, blob_name);
    if (rule == nullptr) {
      CLIO_CO_RETURN;
    }

    // 3. Resolve the prompt template the rule names.
    auto pit = config_.label_prompts_.find(rule->prompt_);
    if (pit == config_.label_prompts_.end()) {
      HLOG(kWarning,
           "Summarizer: rule references unknown prompt '{}', skipping",
           rule->prompt_);
      CLIO_CO_RETURN;
    }

    // 4. Snapshot the blob payload off shared memory into a plain string.
    //    OllamaGenerate blocks on libcurl, so we want a stable buffer that
    //    doesn't share lifetime with the inbound ShmPtr.
    std::string payload;
    if (!task->blob_data_.IsNull() && task->size_ > 0) {
      auto fullptr =
          CLIO_IPC->ToFullPtr<char>(task->blob_data_.template Cast<char>());
      if (fullptr.ptr_) {
        payload.assign(fullptr.ptr_, task->size_);
      }
    }

    // 5. Inference, then store. Both failures are logged, never propagated.
    std::string summary =
        GenerateSummary(config_.label_endpoint_, *rule, pit->second, payload,
                        tag_name, blob_name);
    if (summary.empty()) {
      HLOG(kWarning,
           "Summarizer: produced no output for tag='{}' blob='{}'", tag_name,
           blob_name);
      CLIO_CO_RETURN;
    }
    CLIO_CO_AWAIT(StoreSummary(task->tag_id_, blob_name, task->score_, summary));
  }
  CLIO_CO_RETURN;
  CLIO_TASK_BODY_END
}

clio::run::u64 Runtime::GetWorkRemaining() const {
  // Summarization is inline on the task; nothing is queued behind it.
  return 0;
}

}  // namespace clio::cae::summarizer
