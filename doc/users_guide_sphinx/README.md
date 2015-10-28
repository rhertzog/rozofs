Documentation Source for RozoFS
===============================

This project hosts the source behind [RozoFS Documentation](http://rozofs.github.io/rozofs/master/).

Contributions to the documentation are welcome.  To make changes, submit a pull request
that changes the reStructuredText files in this directory only.

## Building Documentation

### Prerequisites

To install sphinx and the required theme:

```
$ apt-get install sphinx
```
```
$ pip install install sphinx_rtd_theme
```

### Generate Documentation

For build the HTML documentation:
```
$ make sphinx_guide_html
```
For build the PDF documentation:
```
$ make sphinx_guide_pdf
```
For build the LateX documentation:
```
$ make sphinx_guide_latex
```


