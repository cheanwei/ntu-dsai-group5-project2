# Notebooks

**Run `nbstripout --install` before opening any of these — every person, every
clone.** Notebook JSON diffs on every cell execution and embeds outputs;
without stripping, six people editing notebooks in one repo produces continuous
merge conflicts (§10).

    uv run nbstripout --install
    uv run nbstripout --status     # confirm it took

`.gitattributes` is committed, so `*.ipynb filter=nbstripout` reaches everyone
on clone. The filter it names lives in `.git/config`, which does **not** get
cloned — so each person must still run the install themselves. Skipping it
raises no error; git silently passes the notebook through unstripped, and the
first commit with outputs embedded starts the conflicts this rule exists to
prevent.

**One notebook per person — never a shared one.** The four here map to the four
analyses in §9; if two people work the same area, fork a copy under your own
name rather than editing in parallel.

**Notebooks do no cleaning (§6).** They read `olist_marts` through SQLAlchemy
and nothing else — not `raw`, not `staging`. Cleaning here means each analyst
gets different numbers, which is the whole argument for the warehouse.

Owner: lane B2, unblocked once staging exists.
