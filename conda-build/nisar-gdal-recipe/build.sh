#!/bin/bash

set -ex # Exit on error and print commands

rm -rf build/*
mkdir build
cd build

# Configure the build for the Conda Sandbox
# Use $PREFIX for all paths, and ${SHLIB_EXT} for dynamic extension handling
cmake .. ${CMAKE_ARGS} \
    -DGDAL_INCLUDE_DIR="$PREFIX/include" \
    -DGDAL_LIBRARY="$PREFIX/lib/libgdal${SHLIB_EXT}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$PREFIX" \
    -DCMAKE_PREFIX_PATH="$PREFIX" \
    -DCMAKE_VERBOSE_MAKEFILE=ON

# Compile the plugin
make -j${CPU_COUNT}

# Install the plugin into the correct gdalplugins directory
make install

# Verify the plugin actually ended up in the right place
if [ ! -f "$PREFIX/lib/gdalplugins/gdal_NISAR${SHLIB_EXT}" ]; then
    echo "ERROR: Plugin was not installed to $PREFIX/lib/gdalplugins"
    exit 1
fi

# User documentation. The recipe source is frmts/nisar (source: path: ../../frmts/nisar),
# so the repository-level docs are reached through RECIPE_DIR.
REPO_ROOT="$RECIPE_DIR/../.."
DOC_DIR="$PREFIX/share/doc/${PKG_NAME}"
mkdir -p "$DOC_DIR"
# The README links to repository files that are not installed; point those at GitHub
# (spaces percent-encoded) and docs/HOWTO.md at the copy installed next to it.
REPO_URL="https://github.com/JPL-Devin/nisar-hdf5-gdalplugin/blob/main"
sed -E \
    -e "s#\]\(<([^>]+)>\)#](${REPO_URL}/\1)#g" \
    -e "s#\]\(([^)#:<][^):]*)\)#](${REPO_URL}/\1)#g" \
    -e "s#\]\(${REPO_URL}/docs/(HOWTO|AGENT_GUIDE)\.md\)#](\1.md)#g" \
    -e ':a' -e "s#(\]\(${REPO_URL}/[^)]*) ([^)]*\))#\1%20\2#" -e 'ta' \
    "$REPO_ROOT/README.md" > "$DOC_DIR/README.md"
cp "$REPO_ROOT/docs/HOWTO.md" "$DOC_DIR/HOWTO.md"
cp "$REPO_ROOT/docs/AGENT_GUIDE.md" "$DOC_DIR/AGENT_GUIDE.md"
cp "$REPO_ROOT/.agents/skills/nisar-gdal/SKILL.md" "$DOC_DIR/SKILL.md"

echo "SUCCESS: Plugin Built and Installed!"
