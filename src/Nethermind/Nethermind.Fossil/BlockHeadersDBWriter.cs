using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using Nethermind.Core;
using Nethermind.Serialization.Rlp;
using Nethermind.Logging;
        

namespace Nethermind.Fossil 
{
    public class BlockHeadersDBWriter
    {
        private static ILogger _logger;
        private const string DROP_TABLES = "DROP TABLE IF EXISTS blockheaders, transactions;";

        private const string CREATE_BLOCKHEADERS_TABLE = @"
            CREATE TABLE IF NOT EXISTS blockheaders (
            author CHAR(42),
            block_hash CHAR(66) PRIMARY KEY,
            number BIGINT,
            parent_hash CHAR(66),
            beneficiary CHAR(42),
            gas_limit BIGINT NOT NULL,
            gas_used BIGINT NOT NULL,
            timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            extra_data BYTEA NOT NULL,
            difficulty VARCHAR(78) NOT NULL,
            mix_hash CHAR(66),
            nonce VARCHAR(78) NOT NULL,
            uncles_hash CHAR(66),
            transaction_root CHAR(66),
            receipts_root CHAR(66),
            state_root CHAR(66),
            base_fee_per_gas VARCHAR(78),
            withdrawals_root CHAR(66),
            parent_beacon_block_root CHAR(66),
            blob_gas_used VARCHAR(78),
            excess_blob_gas VARCHAR(78),
            total_difficulty VARCHAR(78),
            step BIGINT,
            signature BYTEA
            );";
        
        private const string CREATE_TRANSACTIONS_TABLE = @"
            CREATE TABLE IF NOT EXISTS transactions (
            block_number BIGINT,
            block_hash CHAR(66) REFERENCES blockheaders(block_hash),
            transaction_hash CHAR(66),
            mint VARCHAR(78),
            source_hash CHAR(66),
            nonce VARCHAR(78) NOT NULL,
            transaction_index INTEGER NOT NULL,
            from_addr CHAR(42),
            to_addr CHAR(42),
            value VARCHAR(78) NOT NULL,
            gas_price VARCHAR(78) NOT NULL,
            max_priority_fee_per_gas VARCHAR(78),
            max_fee_per_gas VARCHAR(78),
            gas VARCHAR(78) NOT NULL,
            input BYTEA,
            chain_id VARCHAR(78),
            type SMALLINT NOT NULL,
            v VARCHAR(78)
            );";

        private const string CONNECTION_STRING = @"CONNETION_STRING";

        private static NpgsqlDataSource? _dataSource;

        // SETUP DB


        private BlockHeadersDBWriter(NpgsqlDataSource dataSource) {
            _dataSource = dataSource;
        }

        async public static Task<BlockHeadersDBWriter> SetupBlockHeadersDBWriter(ILogger logger) {
            _logger = logger;
            NpgsqlDataSource dataSource = NpgsqlDataSource.Create(CONNECTION_STRING);
            var dropCommand = dataSource.CreateCommand(DROP_TABLES);
            await dropCommand.ExecuteNonQueryAsync();
            var createBlockheadersTableCommand = dataSource.CreateCommand(CREATE_BLOCKHEADERS_TABLE);
            await createBlockheadersTableCommand.ExecuteNonQueryAsync();
            var createTransactionsTableCommand = dataSource.CreateCommand(CREATE_TRANSACTIONS_TABLE);
            await createTransactionsTableCommand.ExecuteNonQueryAsync();
            _logger.Info("[BlockHeadersDBWriter]: Fossil dbWriter setup complete");
            return new BlockHeadersDBWriter(dataSource);
        }
        
        private string? ULongToHexString(ulong? ul) {
            if (ul == null) {
                return null;
            }
            return string.Format("0x{0:X}", ul);
        }

        public void WriteBinaryToDB(List<Block?> blocks) {
            using (var conn = _dataSource!.OpenConnection()) {
                _logger.Info("[BlockHeadersDBWriter]: Writing headers...");
                using (var headerWriter = conn.BeginBinaryImport(
                    "copy blockheaders from STDIN (FORMAT BINARY)"))
                    {

                        foreach (var block in blocks)
                        {
                            if (block == null) continue;
                            headerWriter.StartRow();
                            headerWriter.Write((object?) block.Author?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write((object?) block.Hash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write(block.Number, NpgsqlTypes.NpgsqlDbType.Bigint);
                            headerWriter.Write((object?) block.ParentHash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write((object?) block.Beneficiary?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write(block.GasLimit, NpgsqlTypes.NpgsqlDbType.Bigint);
                            headerWriter.Write(block.GasUsed, NpgsqlTypes.NpgsqlDbType.Bigint);
                            headerWriter.Write(block.TimestampDate, NpgsqlTypes.NpgsqlDbType.Timestamp);
                            headerWriter.Write(block.ExtraData, NpgsqlTypes.NpgsqlDbType.Bytea);
                            headerWriter.Write(block.Difficulty.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                            headerWriter.Write((object?) block.MixHash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write(ULongToHexString(block.Nonce)!, NpgsqlTypes.NpgsqlDbType.Varchar);
                            headerWriter.Write((object?) block.UnclesHash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write((object?) block.TxRoot?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write((object?) block.ReceiptsRoot?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write((object?) block.StateRoot?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write(block.BaseFeePerGas.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                            headerWriter.Write((object?) block.WithdrawalsRoot?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write((object?) block.ParentBeaconBlockRoot?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                            headerWriter.Write((object?) ULongToHexString(block.BlobGasUsed) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);
                            headerWriter.Write((object?) ULongToHexString(block.ExcessBlobGas) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);
                            headerWriter.Write((object?) block.TotalDifficulty?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);
                            headerWriter.Write((object?) block.Header.AuRaStep ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Bigint);
                            headerWriter.Write((object?) block.Header.AuRaSignature ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Bytea);
                        }
                        headerWriter.Complete();
                    }
                _logger.Info("[BlockHeadersDBWriter]: Writing transactions...");
                using (var transactionWriter = conn.BeginBinaryImport(
                    "copy transactions from STDIN (FORMAT BINARY)"))
                    {
                        foreach (var block in blocks)
                        {
                            if (block == null) continue;
                            int count = 0;
                            foreach (var transaction in block.Transactions)
                            {
                                count++;
                                transactionWriter.StartRow();
                                transactionWriter.Write(block.Number, NpgsqlTypes.NpgsqlDbType.Bigint);
                                transactionWriter.Write((object?) block.Hash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                transactionWriter.Write((object?) transaction.Hash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                transactionWriter.Write(transaction.Mint.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                transactionWriter.Write((object?) transaction.SourceHash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                transactionWriter.Write(transaction.Nonce.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                transactionWriter.Write(count, NpgsqlTypes.NpgsqlDbType.Integer);
                                transactionWriter.Write((object?) transaction.SenderAddress?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                transactionWriter.Write((object?) transaction.To?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                transactionWriter.Write(transaction.Value.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                transactionWriter.Write(transaction.GasPrice.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                transactionWriter.Write(transaction.MaxPriorityFeePerGas.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                transactionWriter.Write(transaction.MaxFeePerGas.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                transactionWriter.Write(transaction.GasPrice.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                transactionWriter.Write(transaction.Data, NpgsqlTypes.NpgsqlDbType.Bytea);
                                transactionWriter.Write((object?) ULongToHexString(transaction.ChainId) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);
                                transactionWriter.Write((object?) ((byte?) transaction.Type) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Smallint);
                                transactionWriter.Write((object?) ULongToHexString(transaction.Signature?.V) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);                            
                        }
                    }
                    transactionWriter.Complete();
                }
                _logger.Info("[BlockHeadersDBWriter]: DB dump complete.");
                conn.Close();
            }
        }
    }
}