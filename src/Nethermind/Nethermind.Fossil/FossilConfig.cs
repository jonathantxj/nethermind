// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

namespace Nethermind.Fossil
{
    public class FossilConfig : IFossilConfig
    {
        public string? ConnectionString { get; set; }
    }
}
