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
        // private ReadOnlyBlockTree _readOnlyBlockTree = null!;
        private ILogger _logger;
        private BlockHeadersDBWriter? _dbWriter;
        public virtual string Name => "Fossil";
        public virtual string Description => "Blockheaders DB Access plugin for Fossil";
        public string Author => "Nethermind";

        public Task Init(INethermindApi nethermindApi)
        {
            _api = nethermindApi ?? throw new ArgumentNullException(nameof(nethermindApi));
            _logger = nethermindApi.LogManager.GetClassLogger();
            _dbWriter = BlockHeadersDBWriter.SetupBlockHeadersDBWriter().Result;

            // IBlockTree blockTree = _api.BlockTree ?? throw new ArgumentNullException(nameof(_api.BlockTree));
            // _readOnlyBlockTree = blockTree.AsReadOnly();

            IDb? headersDb = _api.DbProvider?.HeadersDb;

            if (headersDb == null)
                {
                    _logger.Warn("HeadersDB is null");
                    return Task.CompletedTask;
                }

            int count = 0;
            int chunkSize = 10000;
            long headersCount = headersDb.GatherMetric().Size;
            
            _logger.Info("Writing " + headersCount + " headers: ");

            HeaderDecoder _headerDecoder = new HeaderDecoder();
            DateTime start = DateTime.Now;
            foreach (var headersChunk in headersDb.GetAllValues().Chunk(chunkSize))
                {
                    var writeTasks = headersChunk
                    .Select(entry => {
                        BlockHeader? resultBlockHeader = _headerDecoder.Decode(new RlpStream(entry), RlpBehaviors.None);
                        if (resultBlockHeader != null) {
                            return _dbWriter.WriteToDB(resultBlockHeader);
                        } else {
                            return Task.CompletedTask;
                        }
                    });
                    Task.WaitAll(writeTasks.ToArray());
                    count += chunkSize;
                    _logger.Info(String.Format("Headers Written: {0} -- {1:F2}%", count, (100 * count / headersCount)));
                }
                _logger.Info(String.Format("Time Elapsed: {1:g} (h:mm:ss.ffffff)", count, (DateTime.Now - start)));
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() { return ValueTask.CompletedTask; }
        public Task InitNetworkProtocol()
        {
            return Task.CompletedTask;
        }
        public Task InitRpcModules()
        {
            return Task.CompletedTask;
        }
    }
}