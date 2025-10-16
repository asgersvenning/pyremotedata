# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os, sys, re
sys.path.insert(0, os.path.abspath('../'))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'PyRemoteData'
copyright = '2024, Asger Svenning'
author = 'Asger Svenning'
release = [re.search(r'version\s*=\s*\"([^\"]*)\"', line).group(1) for line in open('../pyproject.toml') if 'version' in line][0]

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.coverage",
    "sphinx.ext.mathjax",
    "sphinx.ext.githubpages",
    "sphinx.ext.autosectionlabel"
]
intersphinx_mapping = {
    'pyremotedata': ("https://asgersvenning.com/pyremotedata/", None),
    'python': ('https://docs.python.org/3', None),  
    "sphinx_docs": ("https://www.sphinx-doc.org/en/master", None),
    "MyST parser docs": ("https://myst-parser.readthedocs.io/en/latest", None)
}

autosummary_generate = True
autosummary_generate_overwrite = True

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_rtype = False

typehints_use_signature = True
typehints_use_signature_return = True
typehints_document_description_target = True
typehints_document_rtype = False
typehints_fully_qualified = False
always_use_bars_union = True

# Improve cross-linking to sections
autosectionlabel_prefix_document = True

# Local TOC depth for sidebars
toc_object_entries = True

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

extensions.append("sphinx_immaterial")
html_theme = 'sphinx_immaterial'
html_css_files = ['custom.css']
html_extra_path = ['.nojekyll']
html_static_path = ['_static']
html_baseurl = ''
html_favicon = 'images/favicon.ico'
html_logo = 'images/cloud_sync.svg'

html_theme_options = {
    "icon": {
        "repo": "fontawesome/brands/github",
        "edit": "material/file-edit-outline",
    },
    # Set the repo location to get a badge with stats
    "repo_url": "https://github.com/asgersvenning/pyremotedata",
    'site_url': 'https://asgersvenning.com/pyremotedata',
    'repo_name': 'PyRemoteData',
    "edit_uri": "blob/main/docs",
    "globaltoc_collapse": True,
    'features': [
        "navigation.expand",
        "navigation.sections",
        # "navigation.tabs",
        "navigation.top",
        "navigation.footer",
        "search.share",
        "search.suggest",
        "toc.follow",
        "toc.sticky",
        "content.tabs.link",
        "content.code.copy",
        "content.action.edit",
        "content.action.view",
        "content.tooltips",
        "announce.dismiss",
    ],
    "palette": [
        {
            "media": "(prefers-color-scheme)",
            "toggle": {
                "icon": "material/brightness-auto",
                "name": "Switch to light mode",
            },
            'scheme' : 'default',
            'primary' : 'blue',
            'accent' : 'light-blue'
        }
    ],
    "toc_title_is_page_title": True,
    "version_dropdown": True,
    # Additional theme options
    'social': [
        {'link': 'https://github.com/asgersvenning/pyremotedata', 'name': 'Source on GitHub', 'icon': 'fontawesome/brands/github'},
        {'icon': 'fontawesome/brands/python', 'link': "https://pypi.org/project/pyremotedata/"}
    ],
}

python_type_aliases = {}

### Custom code to remove custom Sphinx comments from docstrings

import re

def preprocess_docstring(app, what, name, obj, options, lines):
    START_TAG = ".. <Sphinx comment"
    END_TAG = ".. Sphinx comment>"

    docstring = '\n'.join(lines)

    # Find all start and end tag positions
    start_positions = [match.start() for match in re.finditer(START_TAG, docstring)]
    end_positions = [match.start() for match in re.finditer(END_TAG, docstring)]

    comment_blocks = []

    string_index = 0
    while string_index < len(docstring) and len(start_positions) > 0:
        for i, start_position in enumerate(start_positions):
            if start_position >= string_index:
                matched_start = i
                break
        else:
            matched_start = None
        if matched_start is None:
            break
        first_start = start_positions.pop(matched_start)
        start_positions = start_positions[matched_start:]
        for i, end_position in enumerate(end_positions):
            if end_position > first_start:
                matched_end = i
                break
        else:
            matched_end = None
        if matched_end is None:
            following_end = len(docstring)
        else:
            following_end = end_positions.pop(matched_end)
            end_positions = end_positions[matched_end:]
        comment_blocks += [(first_start, following_end)]
        string_index = following_end + len(END_TAG)
    for start, end in reversed(comment_blocks):
        docstring = docstring[:start] + docstring[end+len(END_TAG):]
    lines[:] = docstring.split('\n')
    

def setup(app):
    app.connect('autodoc-process-docstring', preprocess_docstring)