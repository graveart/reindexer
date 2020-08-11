#include "searchengine.h"
#include <tools/stringstools.h>
#include <locale>
#include <string>
#include "core/ft/filters/kblayout.h"
#include "core/ft/filters/translit.h"
#include "estl/string_view.h"

namespace search_engine {
using namespace reindexer;
using std::make_shared;

SearchEngine::SearchEngine() {
	seacher_.AddSeacher(ITokenFilter::Ptr(new Translit));
	seacher_.AddSeacher(ITokenFilter::Ptr(new KbLayout));
	last_max_id_ = 0;
	holder_ = make_shared<BaseHolder>();
	commited_ = false;
}
void SearchEngine::SetConfig(const unique_ptr<FtFuzzyConfig>& cfg) { holder_->SetConfig(cfg); }

void SearchEngine::Rebuild() { holder_.reset(new BaseHolder); }
void SearchEngine::AddData(const reindexer::string_view& src_data, const IdType id, int field, const string& extraWordSymbols) {
	if (commited_) {
		commited_ = false;
		holder_->Clear();
	}
	seacher_.AddIndex(holder_, src_data, id, field, extraWordSymbols);
}
void SearchEngine::Commit() {
	commited_ = true;
	seacher_.Commit(holder_);
}

SearchResult SearchEngine::Search(const FtDSLQuery& dsl) { return seacher_.Compare(holder_, dsl); }

}  // namespace search_engine
