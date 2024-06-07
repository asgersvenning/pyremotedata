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
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode", 
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon"
]
napoleon_google_docstring = True
napoleon_numpy_docstring = False

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_material'
html_css_files = ['custom.css']
html_static_path = ['_static']
html_baseurl = '_build/html/'

html_theme_options = {

    # Set the name of the project to appear in the navigation.
    'nav_title': 'PyRemoteData',

    # # Specify a base_url used to generate sitemap.xml. If not
    # # specified, then no sitemap will be built.
    # 'base_url': 'https://project.github.io/project',

    # # Set the color and the accent color
    # 'color_primary': 'blue',
    # 'color_accent': 'light-blue',

    # Set the repo location to get a badge with stats
    'repo_url': 'https://github.com/asgersvenning/pyremotedata',
    'repo_name': 'pyremotedata',

    # Visible levels of the global TOC; -1 means unlimited
    'globaltoc_depth': 3,
    # If False, expand all TOC entries
    'globaltoc_collapse': False,
    # If True, show hidden TOC entries
    'globaltoc_includehidden': False,
}

intersphinx_mapping = {
    'pyremotedata': ("https://asgersvenning.github.io/pyremotedata/" + html_baseurl, None),
    'python': ('https://docs.python.org/3', None),
    'torch': ('https://pytorch.org/docs/stable/', None),
}

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
        # Get the first unused start tag after the current string index
        for i, start_position in enumerate(start_positions):
            if start_position >= string_index:
                matched_start = i
                break
        else:
            matched_start = None
        
        # If there are no start tags after the current string index, then we are done
        if matched_start is None:
            break

        # Get the position of the first start tag
        first_start = start_positions.pop(matched_start)
        # And remove all start tags that are before the first start tag (we don't need to consider them anymore)
        start_positions = start_positions[matched_start:]

        # Find the first end tag that is after the start tag
        for i, end_position in enumerate(end_positions):
            if end_position > first_start:
                matched_end = i
                break
        else:
            matched_end = None
        
        # If there are no end tags after the start tag, then we insert an implicit end tag at the end of the docstring
        if matched_end is None:
            following_end = len(docstring)
        # Otherwise, we use the matched end tag
        else:
            following_end = end_positions.pop(matched_end)
            # And remove all end tags that are before the start tag (we don't need to consider them anymore)
            end_positions = end_positions[matched_end:]
        
        # Save the comment block start and end positions
        comment_blocks += [(first_start, following_end)]

        # Move the string index to the end of the comment block
        string_index = following_end + len(END_TAG)
    
    # Remove the comment blocks from the docstring
    for start, end in reversed(comment_blocks):
        docstring = docstring[:start] + docstring[end+len(END_TAG):]
    
    lines[:] = docstring.split('\n')
    
def setup(app):
    app.connect('autodoc-process-docstring', preprocess_docstring)