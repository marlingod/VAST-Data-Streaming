"""Helper to render styled HTML in Streamlit.

Streamlit 1.52+ renders st.html() inside iframes, so CSS from the parent
page does not cascade into those blocks.  This module provides a single
``styled_html`` function that wraps content in the project's stylesheet
so every st.html() call gets consistent styling.
"""

from __future__ import annotations

import pathlib

import streamlit as st

_CSS_PATH = pathlib.Path(__file__).resolve().parent.parent / "assets" / "styles.css"
_CSS_CACHE: str | None = None


def _get_css() -> str:
    global _CSS_CACHE
    if _CSS_CACHE is None:
        if _CSS_PATH.exists():
            _CSS_CACHE = _CSS_PATH.read_text()
        else:
            _CSS_CACHE = ""
    return _CSS_CACHE


def styled_html(html: str) -> None:
    """Render *html* inside ``st.html`` with the project stylesheet injected."""
    css = _get_css()
    st.html(f"<style>{css}</style>\n{html}")
