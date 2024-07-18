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
            author CHAR(42),
            block_hash CHAR(66) PRIMARY KEY,
            number BIGINT UNIQUE,
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
            transaction_hash CHAR(66) PRIMARY KEY,
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

        private const string INSERT_BLOCKHEADERS = @"INSERT INTO blockheaders (
            author, block_hash, number, parent_hash, beneficiary, gas_limit, gas_used, 
            timestamp, extra_data, difficulty, mix_hash, nonce, uncles_hash, 
            transaction_root, receipts_root, state_root, base_fee_per_gas, 
            withdrawals_root, parent_beacon_block_root, blob_gas_used, 
            excess_blob_gas, total_difficulty, step, signature
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
                $19, $20, $21, $22, $23, $24)
        ON CONFLICT (block_hash) DO NOTHING";

        private static NpgsqlDataSource? _dataSource;

        private BlockHeadersDBWriter(NpgsqlDataSource dataSource)
        {
            _dataSource = dataSource;
        }

        async public static Task<BlockHeadersDBWriter> SetupBlockHeadersDBWriter(ILogger logger, string connectionString)
        {
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

        private string? ULongToHexString(ulong? ul)
        {
            if (ul == null)
            {
                return null;
            }
            return string.Format("0x{0:X}", ul);
        }


        public async Task<bool> WriteBlockToDB(Block block, IEthereumEcdsa ethereumEcdsa)
        {
            int maxRetries = 50;
            for (int attempt = 0; attempt < maxRetries; attempt++)
            {
                try
                {
                    await using var conn = await _dataSource!.OpenConnectionAsync();
                    await using var tx = await conn.BeginTransactionAsync();
                    try
                    {
                        await using var cmd = new NpgsqlCommand(INSERT_BLOCKHEADERS, conn, tx)
                        {
                            Parameters =
                            {
                                new() { Value = (object?)block.Author?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = (object?)block.Hash?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = block.Number, NpgsqlDbType = NpgsqlDbType.Bigint },
                                new() { Value = (object?)block.ParentHash?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = (object?)block.Beneficiary?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = block.GasLimit, NpgsqlDbType = NpgsqlDbType.Bigint },
                                new() { Value = block.GasUsed, NpgsqlDbType = NpgsqlDbType.Bigint },
                                new() { Value = block.TimestampDate, NpgsqlDbType = NpgsqlDbType.Timestamp },
                                new() { Value = block.ExtraData, NpgsqlDbType = NpgsqlDbType.Bytea },
                                new() { Value = block.Difficulty.ToString(), NpgsqlDbType = NpgsqlDbType.Varchar },
                                new() { Value = (object?)block.MixHash?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = ULongToHexString(block.Nonce)!, NpgsqlDbType = NpgsqlDbType.Varchar },
                                new() { Value = (object?)block.UnclesHash?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = (object?)block.TxRoot?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = (object?)block.ReceiptsRoot?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = (object?)block.StateRoot?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = block.BaseFeePerGas.ToString(), NpgsqlDbType = NpgsqlDbType.Varchar },
                                new() { Value = (object?)block.WithdrawalsRoot?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = (object?)block.ParentBeaconBlockRoot?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Char },
                                new() { Value = (object?)ULongToHexString(block.BlobGasUsed) ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Varchar },
                                new() { Value = (object?)ULongToHexString(block.ExcessBlobGas) ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Varchar },
                                new() { Value = (object?)block.TotalDifficulty?.ToString() ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Varchar },
                                new() { Value = (object?)block.Header.AuRaStep ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bigint },
                                new() { Value = (object?)block.Header.AuRaSignature ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bytea }
                            }
                        };
                        _logger.Info("Executing");
                        int rowsAffected = await cmd.ExecuteNonQueryAsync();
                        _logger.Info("Executed");

                        if (rowsAffected > 0)
                        {
                            int count = 0;
                            _logger.Info("Importing");
                            await using (var transactionWriter = await conn.BeginBinaryImportAsync(
                                "copy transactions from STDIN (FORMAT BINARY)"))
                            {
                                foreach (var transaction in block.Transactions)
                                {
                                    count++;
                                    await transactionWriter.StartRowAsync();
                                    await transactionWriter.WriteAsync(block.Number, NpgsqlTypes.NpgsqlDbType.Bigint);
                                    await transactionWriter.WriteAsync((object?)block.Hash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                    await transactionWriter.WriteAsync((object?)transaction.Hash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                    await transactionWriter.WriteAsync(transaction.Mint.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                    await transactionWriter.WriteAsync((object?)transaction.SourceHash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                    await transactionWriter.WriteAsync(transaction.Nonce.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                    await transactionWriter.WriteAsync(count, NpgsqlTypes.NpgsqlDbType.Integer);
                                    await transactionWriter.WriteAsync((object?)ethereumEcdsa.RecoverAddress(transaction)?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                    await transactionWriter.WriteAsync((object?)transaction.To?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                                    await transactionWriter.WriteAsync(transaction.Value.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                    await transactionWriter.WriteAsync(transaction.GasPrice.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                    await transactionWriter.WriteAsync(transaction.MaxPriorityFeePerGas.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                    await transactionWriter.WriteAsync(transaction.MaxFeePerGas.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                    await transactionWriter.WriteAsync(transaction.GasPrice.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                                    await transactionWriter.WriteAsync(transaction.Data, NpgsqlTypes.NpgsqlDbType.Bytea);
                                    await transactionWriter.WriteAsync((object?)ULongToHexString(transaction.ChainId) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);
                                    await transactionWriter.WriteAsync((object?)((byte?)transaction.Type) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Smallint);
                                    await transactionWriter.WriteAsync((object?)ULongToHexString(transaction.Signature?.V) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);
                                }
                                _logger.Info("Completing");
                                await transactionWriter.CompleteAsync();
                                _logger.Info("Completed");
                            }
                        }
                        _logger.Info("Commiting");
                        await tx.CommitAsync();
                        _logger.Info("Commited");
                        return true;
                    }
                    catch (Exception e)
                    {
                        await tx.RollbackAsync();
                        if (attempt == maxRetries - 1)
                        {
                            _logger.Error($"[BlockHeadersDBWriter]: Failed to write block {block.Number} after {maxRetries} attempts. {e}");
                            return false;
                        }
                        _logger.Warn($"[BlockHeadersDBWriter]: Write error with block {block.Number}. Retrying. {e}");
                    }
                }
                catch (Exception e)
                {
                    if (attempt == maxRetries - 1)
                    {
                        _logger.Error($"[BlockHeadersDBWriter]: Failed to open connection or begin transaction for block {block.Number} after {maxRetries} attempts. {e}");
                        return false;
                    }
                    _logger.Warn($"[BlockHeadersDBWriter]: Error opening connection or beginning transaction for block {block.Number}. Retrying. {e}");
                }
            }
            return false;
        }
    }
}
