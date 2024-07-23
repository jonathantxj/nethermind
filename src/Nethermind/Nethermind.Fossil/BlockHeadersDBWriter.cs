using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Nethermind.Core;
using Nethermind.Crypto;
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
            block_hash CHAR(66) PRIMARY KEY,
            number BIGINT UNIQUE,
            gas_limit BIGINT NOT NULL,
            gas_used BIGINT NOT NULL,
            nonce VARCHAR(78) NOT NULL,
            transaction_root CHAR(66),
            receipts_root CHAR(66),
            state_root CHAR(66)
            );";
        
        private const string CREATE_TRANSACTIONS_TABLE = @"
            CREATE TABLE IF NOT EXISTS transactions (
            block_number BIGINT REFERENCES blockheaders(number),
            transaction_hash CHAR(66) PRIMARY KEY,
            from_addr CHAR(42),
            to_addr CHAR(42),
            value VARCHAR(78) NOT NULL,
            gas_price VARCHAR(78) NOT NULL,
            max_priority_fee_per_gas VARCHAR(78),
            max_fee_per_gas VARCHAR(78),
            transaction_index INTEGER NOT NULL,
            gas VARCHAR(78) NOT NULL,
            chain_id VARCHAR(78)
            );";

        private const string INSERT_BLOCKHEADERS = @"INSERT INTO blockheaders (
            block_hash, number, gas_limit, gas_used, nonce,
            transaction_root, receipts_root, state_root
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (block_hash) DO NOTHING";

        private static NpgsqlDataSource? _dataSource;

        private BlockHeadersDBWriter(NpgsqlDataSource dataSource) {
            _dataSource = dataSource;
        }

        async public static Task<BlockHeadersDBWriter> SetupBlockHeadersDBWriter(ILogger logger, string connectionString) {
            _logger = logger;
            NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
            // var dropCommand = dataSource.CreateCommand(DROP_TABLES);
            // await dropCommand.ExecuteNonQueryAsync();
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

        public void WriteBinaryToDB(IEnumerable<Block?> blocks, IEthereumEcdsa ethereumEcdsa) {
            var firstBlock = blocks.First()?.Number;

            Parallel.ForEach(
                blocks,
                new ParallelOptions { MaxDegreeOfParallelism = 1000 },
                block => {
                    if (block == null) return;
                        using (var conn = _dataSource!.OpenConnection()) {
                            for (int i = 0; i < 50; i++) 
                            {
                            using var tx = conn.BeginTransaction();
                            try
                            {
                                using var cmd = new NpgsqlCommand(INSERT_BLOCKHEADERS, conn, tx)
                                    {
                                        Parameters =
                                        {
                                            new() { Value = (object?)block.Hash?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                            new() { Value = block.Number, NpgsqlDbType = NpgsqlDbType.Bigint },
                                            new() { Value = block.GasLimit, NpgsqlDbType = NpgsqlDbType.Bigint },
                                            new() { Value = block.GasUsed, NpgsqlDbType = NpgsqlDbType.Bigint },
                                            new() { Value = ULongToHexString(block.Nonce)!, NpgsqlDbType = NpgsqlDbType.Varchar },
                                            new() { Value = (object?)block.TxRoot?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                            new() { Value = (object?)block.ReceiptsRoot?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                            new() { Value = (object?)block.StateRoot?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char }
                                        }
                                    };
                                int rowsAffected = cmd.ExecuteNonQuery();

                                if (rowsAffected > 0) {
                                    int count = 0;
                                    using (var transactionWriter = conn.BeginBinaryImport(
                                        "copy transactions from STDIN (FORMAT BINARY)"))
                                    {
                                        foreach (var transaction in block.Transactions)
                                        {
                                            count++;
                                            transactionWriter.StartRow();
                                            transactionWriter.Write(block.Number, NpgsqlTypes.NpgsqlDbType.Bigint);
                                            transactionWriter.Write((object?) transaction.Hash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                            transactionWriter.Write((object?) ethereumEcdsa.RecoverAddress(transaction)?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                            transactionWriter.Write((object?) transaction.To?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                            transactionWriter.Write(transaction.Value.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                            transactionWriter.Write(transaction.GasPrice.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                            transactionWriter.Write(transaction.MaxPriorityFeePerGas.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                            transactionWriter.Write(transaction.MaxFeePerGas.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                            transactionWriter.Write(transaction.GasLimit.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                            transactionWriter.Write(count, NpgsqlTypes.NpgsqlDbType.Integer);
                                            transactionWriter.Write((object?) ULongToHexString(transaction.ChainId) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);                      
                                        }
                                        transactionWriter.Complete();
                                    }
                                }
                                
                                tx.Commit();
                                conn.Close();
                                return;
                            }
                            catch (Exception e)
                            {
                                _logger.Warn($"write error {e}");
                                tx.Rollback();
                            }
                        }
                        conn.Close();
                        throw new Exception($"Restart from {firstBlock}");
                    }
                }
            );
            _logger.Info($"[BlockHeadersDBWriter]: Finished writing blocks: Last: {blocks.Last()?.Number}");
        }
    }
}