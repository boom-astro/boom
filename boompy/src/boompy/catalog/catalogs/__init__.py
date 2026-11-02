"""One module per catalog, and nothing else.

Every module in here is a catalog, which is what lets `registry` build the
index by listing this directory rather than from a list someone has to
remember to extend. Shared machinery -- the HTTP helpers, the FITS conversion,
the chunk interface, the CLI -- lives one level up in `boompy.catalog`.

The file name is not the slug: a slug is kebab-case and may start with a digit
(`2mass`, `ned-lvs`), neither of which a module name can be. Each module
declares its own slug as `ID`, and that is what the index is keyed on.
"""
