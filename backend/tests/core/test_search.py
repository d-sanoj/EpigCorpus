"""The keyword matches whole words, not substrings of them.

The case that prompted this: searching "tree" matched the inscription whose
only word is "Matree", which left a single-place result that blanked the page.
"tree" must match "tree" and nothing longer that contains it.
"""

import pandas as pd

from epigcorpus.core.search import apply_filters, filter_inscriptions


def _frame(*texts: str) -> pd.DataFrame:
    return pd.DataFrame({"text": list(texts)})


def test_tree_does_not_match_matree():
    frame = _frame("Matree", "Matree[3]")
    assert filter_inscriptions(frame, "text", "tree").empty
    assert apply_filters(frame, "text", "tree", {}).empty


def test_tree_matches_tree_wherever_it_stands_alone():
    frame = _frame("tree", "a tree grows", "under the tree", "TREE", "Tree")
    found = filter_inscriptions(frame, "text", "tree")
    assert found["text"].tolist() == ["tree", "a tree grows", "under the tree", "TREE", "Tree"]


def test_a_word_inside_another_word_does_not_match():
    frame = _frame("trees", "treehouse", "tree2", "tree_house", "convixit", "vixit")
    found = filter_inscriptions(frame, "text", "vixit")
    assert found["text"].tolist() == ["vixit"]
    assert filter_inscriptions(frame, "text", "tree").empty


def test_punctuation_and_hyphens_are_boundaries():
    frame = _frame("tree.", "(tree)", "tree-house", "tree·x", "tree, tree")
    found = filter_inscriptions(frame, "text", "tree")
    assert found["text"].tolist() == ["tree.", "(tree)", "tree-house", "tree·x", "tree, tree"]


def test_a_greek_letter_next_to_the_term_is_part_of_a_word():
    frame = _frame("αtree", "treeα", "ἡ νίκη", "νίκης")
    assert filter_inscriptions(frame, "text", "tree").empty
    assert filter_inscriptions(frame, "text", "νίκη")["text"].tolist() == ["ἡ νίκη"]


def test_a_phrase_matches_only_as_a_phrase():
    frame = _frame("a tree house here", "a treehouse here", "tree house")
    found = filter_inscriptions(frame, "text", "tree house")
    assert found["text"].tolist() == ["a tree house here", "tree house"]


def test_the_term_is_not_a_regex():
    frame = _frame("a.b", "axb", "c++", "cxx")
    assert filter_inscriptions(frame, "text", "a.b")["text"].tolist() == ["a.b"]
    assert filter_inscriptions(frame, "text", "c++")["text"].tolist() == ["c++"]
