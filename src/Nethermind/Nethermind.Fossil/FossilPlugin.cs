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
        public virtual string Name => "Fossil";
        public virtual string Description => "Blockheaders DB Access plugin for Fossil";
        public string Author => "Nethermind";

        public Task Init(INethermindApi nethermindApi)
        {
            _api = nethermindApi ?? throw new ArgumentNullException(nameof(nethermindApi));
            _logger = nethermindApi.LogManager.GetClassLogger();
            _dbWriter = BlockHeadersDBWriter.SetupBlockHeadersDBWriter().Result;

            IDb? headersDb = _api.DbProvider?.HeadersDb;

            if (headersDb == null)
                {
                    _logger.Warn("HeadersDB is null");
                    return Task.CompletedTask;
                }

            HeaderDecoder _headerDecoder = new HeaderDecoder();

            var blockHeaders = headersDb.GetAllValues().Select(
                entry => _headerDecoder.Decode(new RlpStream(entry), RlpBehaviors.None)
                );
            _dbWriter.WriteBinaryToDB(blockHeaders);

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