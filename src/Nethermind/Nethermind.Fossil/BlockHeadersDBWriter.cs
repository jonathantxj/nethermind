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
        // SETUP DB
        private static NpgsqlDataSource? _dataSource;
        private static string connectionString = @"Host=localhost;
        Port=5432;
        Database=blockheaders;
        User Id=jonathantan;
        Password=;";

        private BlockHeadersDBWriter(NpgsqlDataSource dataSource) {
            _dataSource = dataSource;
        }

        async public static Task<BlockHeadersDBWriter> SetupBlockHeadersDBWriter() {
            NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
            // var dropCommand = dataSource.CreateCommand(DROP_BLOCKHEADERS_TABLE);
            // await dropCommand.ExecuteNonQueryAsync();
            var createCommand = dataSource.CreateCommand(CREATE_BLOCKHEADERS_TABLE);
            await createCommand.ExecuteNonQueryAsync();
            return new BlockHeadersDBWriter(dataSource);
        }

        private const string DROP_BLOCKHEADERS_TABLE = "DROP TABLE IF EXISTS blockheaders;";

        private const string CREATE_BLOCKHEADERS_TABLE = @"
            CREATE TABLE IF NOT EXISTS blockheaders (
            block_hash CHAR(66),
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
            bloom CHAR(512),
            base_fee_per_gas VARCHAR(78) NOT NULL,
            withdrawals_root CHAR(66),
            parent_beacon_block_root CHAR(66),
            blob_gas_used VARCHAR(78),
            excess_blob_gas VARCHAR(78),
            is_post_merge BOOLEAN NOT NULL,
            total_difficulty VARCHAR(78)
            );";
        
        private const string ADD_BLOCKHEADER_IF_NOT_EXISTS = @"
            INSERT INTO blockheaders
                VALUES (@hash, @number, @parent_hash, @beneficiary_addr, @gas_limit, @gas_used,
                @timestamp, @extra_data, @difficulty, @mix_hash, @nonce, @uncles_hash, @tx_root,
                @receipt_root, @state_root, @bloom, @base_fee_per_gas, @withdrawals_root,
                @parent_beacon_block_root, @blob_gas_used, @excess_blob_gas, @is_post_merge, @total_difficulty)
            ON CONFLICT DO NOTHING;";

        private string? ULongToHexString(ulong? ul) {
            if (ul == null) {
                return null;
            }
            return string.Format("0x{0:X4}", ul);
        }

        public async Task WriteToDB(BlockHeader blockHeader)
        {
            if (_dataSource == null) return;
            
            await using (var conn = await _dataSource.OpenConnectionAsync())
            await using (var cmd = new NpgsqlCommand(ADD_BLOCKHEADER_IF_NOT_EXISTS, conn))
            {
                cmd.Parameters.AddWithValue("hash", (object?) blockHeader.Hash?.ToString() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("number", blockHeader.Number);
                cmd.Parameters.AddWithValue("parent_hash", (object?) blockHeader.ParentHash?.ToString() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("beneficiary_addr", (object?) blockHeader.Beneficiary?.ToString(true) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("gas_limit", blockHeader.GasLimit);
                cmd.Parameters.AddWithValue("gas_used", blockHeader.GasUsed);
                cmd.Parameters.AddWithValue("timestamp", blockHeader.TimestampDate);
                cmd.Parameters.AddWithValue("extra_data", blockHeader.ExtraData);
                cmd.Parameters.AddWithValue("difficulty", blockHeader.Difficulty.ToString());
                cmd.Parameters.AddWithValue("mix_hash", (object?) blockHeader.MixHash?.ToString() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("nonce", ULongToHexString(blockHeader.Nonce)!);
                cmd.Parameters.AddWithValue("uncles_hash", (object?) blockHeader.UnclesHash?.ToString() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("tx_root", (object?) blockHeader.TxRoot?.ToString() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("receipt_root", (object?) blockHeader.ReceiptsRoot?.ToString() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("state_root", (object?) blockHeader.StateRoot?.ToString() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("bloom", (object?) blockHeader.Bloom?.ToString() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("base_fee_per_gas", blockHeader.BaseFeePerGas.ToString());
                cmd.Parameters.AddWithValue("withdrawals_root", (object?) blockHeader.WithdrawalsRoot?.ToString() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("parent_beacon_block_root", (object?) blockHeader.ParentBeaconBlockRoot?.ToString() ?? DBNull.Value);
                cmd.Parameters.AddWithValue("blob_gas_used", (object?) ULongToHexString(blockHeader.BlobGasUsed) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("excess_blob_gas", (object?) ULongToHexString(blockHeader.ExcessBlobGas) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("is_post_merge", blockHeader.IsPostMerge);
                cmd.Parameters.AddWithValue("total_difficulty", (object?) blockHeader.TotalDifficulty?.ToString() ?? DBNull.Value);
                
                await cmd.ExecuteNonQueryAsync();
                conn.Close();
            }
            return;
        }
    }
}