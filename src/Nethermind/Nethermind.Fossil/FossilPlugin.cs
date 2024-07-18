using System;
using System.Threading;
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
        private static SemaphoreSlim? _pool;
        public virtual string Name => "Fossil";
        public virtual string Description => "Block DB Access plugin for Fossil";
        public string Author => "Nethermind";

        const int MAX_THREADS = 1500;
        public Task Init(INethermindApi nethermindApi)
        {
            _api = nethermindApi ?? throw new ArgumentNullException(nameof(nethermindApi));

            var (getFromAPi, _) = _api.ForInit;
            var _fossilConfig = getFromAPi.Config<IFossilConfig>();

            var connectionString = _fossilConfig.ConnectionString;
            if (connectionString == null)
            {
                _logger.Info($"{nameof(FossilPlugin)} disabled");
                return Task.CompletedTask;
            }

            _logger = nethermindApi.LogManager.GetClassLogger();
            _logger.Info($"{nameof(FossilPlugin)} enabled");

            _blockTree = nethermindApi.BlockTree!;
            _dbWriter = BlockHeadersDBWriter.SetupBlockHeadersDBWriter(_logger, connectionString).Result;
            IDb? blockDb = _api.DbProvider?.BlocksDb;

            _pool = new SemaphoreSlim(MAX_THREADS, MAX_THREADS);

            if (blockDb == null)
            {
                _logger.Info("blocksDB is null");
                return Task.CompletedTask;
            }

            var lastBlock = 11230241;
            var chunks = blockDb.GetAllValues().Skip(lastBlock + 1).Chunk(100_000);
            _logger.Info("Skipped");

            foreach (var chunk in chunks)
            {
                var tasks = chunk.AsParallel().Select(rlpBlock =>
                {
                    BlockDecoder blockDecoder = new BlockDecoder();
                    var block = blockDecoder.Decode(new RlpStream(rlpBlock), RlpBehaviors.None);
                    if (block == null || !_blockTree.IsMainChain(block.Header) || block.Number <= lastBlock)
                        return Task.CompletedTask;
                    _logger.Info($"Decoded block {block.Number}");

                    return Task.Run(async () =>
                    {
                        _logger.Info($"Waiting for pool for block {block.Number}");
                        _pool.Wait();
                        _logger.Info($"Running for block {block.Number}");
                        try
                        {
                            _logger.Info($"Writing for block {block.Number}");
                            var res = await _dbWriter.WriteBlockToDB(block, _api.EthereumEcdsa!);
                            if (!res)
                            {
                                _logger.Warn($"Error from {block.Number}");
                                throw new Exception($"Error from {block.Number}");
                            }
                            else
                            {
                                _logger.Info($"[FossilPlugin]: Finished writing block: {block.Number}");
                            }
                        }
                        finally
                        {
                            _pool.Release();
                        }
                    });
                }).ToList();
                Task.WaitAll(tasks.ToArray());
            }
            return Task.CompletedTask;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
