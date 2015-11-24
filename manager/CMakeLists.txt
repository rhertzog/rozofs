# Copyright (c) 2013 Fizians SAS. <http://www.fizians.com>
# This file is part of Rozofs.
#
# Rozofs is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published
# by the Free Software Foundation, version 2.
#
# Rozofs is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.

if (NOT PYTHON_NOTFOUND)

    set(DEPS        "${CMAKE_CURRENT_SOURCE_DIR}/rozofs/__init__.py")
    set(OUTPUT      "${CMAKE_CURRENT_BINARY_DIR}/timestamp")

    add_custom_command(OUTPUT ${OUTPUT}
                       COMMAND ${PYTHON_EXECUTABLE} setup.py --quiet build
                       COMMAND ${CMAKE_COMMAND} -E touch ${OUTPUT}
                       DEPENDS ${DEPS})

    add_custom_target(manager ALL DEPENDS ${OUTPUT})

    install(CODE "execute_process (COMMAND cmake -DSETUP=${CMAKE_BINARY_DIR}/manager/setup.py -DDEBIAN=${DEBIAN_LAYOUT} -DPREFIX=${CMAKE_INSTALL_PREFIX} -DMANIFEST=${PROJECT_BINARY_DIR}/manager_install_manifest.txt -DWDIR=${CMAKE_CURRENT_BINARY_DIR} -DPYTHON_EXECUTABLE=${PYTHON_EXECUTABLE} -P ${PROJECT_SOURCE_DIR}/cmake/setup.cmake)")

endif(NOT PYTHON_NOTFOUND)
