using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Nethermind.Api;
using Nethermind.Api.Extensions;
using Nethermind.Blockchain;
using Nethermind.Core;
using Nethermind.Db;
using Nethermind.Serialization.Rlp;
using Nethermind.Logging;
        

namespace Nethermind.Fossil 
{
    public class FossilPlugin : INethermindPlugin
    {
        private INethermindApi _api = null!;
        private ILogger _logger;
        private BlockHeadersDBWriter? _dbWriter;
        private IBlockTree? _blockTree;
        public virtual string Name => "Fossil";
        public virtual string Description => "Block DB Access plugin for Fossil";
        public string Author => "Nethermind";

        public Task Init(INethermindApi nethermindApi)
        {
            _api = nethermindApi ?? throw new ArgumentNullException(nameof(nethermindApi));

            var (getFromAPi, _) = _api.ForInit;
            var _fossilConfig = getFromAPi.Config<IFossilConfig>();

            var connectionString = _fossilConfig.ConnectionString;
            if (connectionString == null) {
                _logger.Info($"{nameof(FossilPlugin)} disabled");
                return Task.CompletedTask;
            }

            _logger = nethermindApi.LogManager.GetClassLogger();
            _logger.Info($"{nameof(FossilPlugin)} enabled");

            _blockTree = nethermindApi.BlockTree!;
            _dbWriter = BlockHeadersDBWriter.SetupBlockHeadersDBWriter(_logger, connectionString).Result;
            IDb? blockDb = _api.DbProvider?.BlocksDb;

            if (blockDb == null)
                {
                    _logger.Warn("BlocksDB is null");
                    return Task.CompletedTask;
                }

            BlockDecoder _blockDecoder = new BlockDecoder();

            var blocks = blockDb.GetAllValues().Skip(1).Select(
                    entry => {
                        var block = _blockDecoder.Decode(new RlpStream(entry), RlpBehaviors.None);
                        if (block == null || !_blockTree.IsMainChain(block.Header)) return null;

                        Parallel.ForEach(block.Transactions, tx =>
                        {
                            tx.SenderAddress ??= _api.EthereumEcdsa?.RecoverAddress(tx);
                        });
                        return block;
                    }
                ).ToList();

            _dbWriter.WriteBinaryToDB(blocks);

            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}