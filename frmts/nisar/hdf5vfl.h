/*
 * Copyright 2025, California Institute of Technology.
 * All rights reserved. U.S. Government sponsorship acknowledged.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/* The HDF5 Virtual File Layer below is derived from GDAL's frmts/hdf5/hdf5vfl.h
 * and remains under its original MIT license: */
/******************************************************************************
 *
 * Project:  Hierarchical Data Format Release 5 (HDF5)
 * Authors:  Denis Nadeau <denis.nadeau@gmail.com>
 * Sam Gillingham <gillingham.sam@gmail.com>
 *
 ******************************************************************************
 * Copyright (c) 2008-2018, Even Rouault <even.rouault at spatialys.com>
 * Copyright 2025 California Institute of Technology (NISAR adaptation).
 * U.S. Government sponsorship acknowledged.
 *
 * SPDX-License-Identifier: MIT
 ****************************************************************************/

// This file contains the Virtual File Layer implementation that calls through
// to the VSI functions and should be included by HDF5 based drivers that wish
// to use the VFL for /vsi file system support.

// hdf5vfl.h
#ifndef NISAR_HDF5VFL_H_INCLUDED_
#define NISAR_HDF5VFL_H_INCLUDED_

#include "cpl_port.h"
#include <hdf5.h>

// --------------------------------------------------------------------------
// CRITICAL: Namespace isolation for out-of-tree plugins.
// Prevents symbol collision with GDAL's internal HDF5/NetCDF drivers.
// --------------------------------------------------------------------------
namespace NisarVFL
{
// We only expose the two functions the rest of your plugin actually needs to see.
// All the heavy lifting (structs, static functions) stays hidden inside hdf5vfl.cpp
hid_t HDF5VFLGetFileDriver();
void HDF5VFLUnloadFileDriver();
}  // namespace NisarVFL

#endif /* NISAR_HDF5VFL_H_INCLUDED_ */
