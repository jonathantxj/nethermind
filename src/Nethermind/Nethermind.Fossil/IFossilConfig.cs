// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Config;

namespace Nethermind.Fossil
{
    [ConfigCategory(DisabledForCli = false, HiddenFromDocs = true)]
    public interface IFossilConfig : IConfig
    {
        [ConfigItem(Description = "If set, blockheaders and transactions will be written to the database provided")]
        public string? ConnectionString { get; set; }
    }
}
