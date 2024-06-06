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

        private static const string CONNECTION_STRING = @"Host=localhost;
        Port=5432;
        Database=blockheaders;
        User Id=jonathantan;
        Password=;";

        private static NpgsqlDataSource? _dataSource;

        // SETUP DB


        private BlockHeadersDBWriter(NpgsqlDataSource dataSource) {
            _dataSource = dataSource;
        }

        async public static Task<BlockHeadersDBWriter> SetupBlockHeadersDBWriter() {
            NpgsqlDataSource dataSource = NpgsqlDataSource.Create(CONNECTION_STRING);
            var dropCommand = dataSource.CreateCommand(DROP_BLOCKHEADERS_TABLE);
            await dropCommand.ExecuteNonQueryAsync();
            var createCommand = dataSource.CreateCommand(CREATE_BLOCKHEADERS_TABLE);
            await createCommand.ExecuteNonQueryAsync();
            return new BlockHeadersDBWriter(dataSource);
        }
        
        private string? ULongToHexString(ulong? ul) {
            if (ul == null) {
                return null;
            }
            return string.Format("0x{0:X4}", ul);
        }

        public void WriteBinaryToDB(IEnumerable<BlockHeader?> blockHeaders) {

            using (var conn = _dataSource!.OpenConnection())
            using (var writer = conn.BeginBinaryImport(
                "copy blockheaders from STDIN (FORMAT BINARY)"))
                {
                    foreach (var blockHeader in blockHeaders)
                    {
                        if (blockHeader == null) continue;
                        writer.StartRow();
                        writer.Write((object?) blockHeader.Hash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write(blockHeader.Number, NpgsqlTypes.NpgsqlDbType.Bigint);
                        writer.Write((object?) blockHeader.ParentHash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write((object?) blockHeader.Beneficiary?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write(blockHeader.GasLimit, NpgsqlTypes.NpgsqlDbType.Bigint);
                        writer.Write(blockHeader.GasUsed, NpgsqlTypes.NpgsqlDbType.Bigint);
                        writer.Write(blockHeader.TimestampDate, NpgsqlTypes.NpgsqlDbType.Timestamp);
                        writer.Write(blockHeader.ExtraData, NpgsqlTypes.NpgsqlDbType.Bytea);
                        writer.Write(blockHeader.Difficulty.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                        writer.Write((object?) blockHeader.MixHash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write(ULongToHexString(blockHeader.Nonce)!, NpgsqlTypes.NpgsqlDbType.Varchar);
                        writer.Write((object?) blockHeader.UnclesHash?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write((object?) blockHeader.TxRoot?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write((object?) blockHeader.ReceiptsRoot?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write((object?) blockHeader.StateRoot?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write((object?) blockHeader.Bloom?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write(blockHeader.BaseFeePerGas.ToString(), NpgsqlTypes.NpgsqlDbType.Varchar);
                        writer.Write((object?) blockHeader.WithdrawalsRoot?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write((object?) blockHeader.ParentBeaconBlockRoot?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Char);
                        writer.Write((object?) ULongToHexString(blockHeader.BlobGasUsed) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);
                        writer.Write((object?) ULongToHexString(blockHeader.ExcessBlobGas) ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);
                        writer.Write( blockHeader.IsPostMerge, NpgsqlTypes.NpgsqlDbType.Boolean);
                        writer.Write((object?) blockHeader.TotalDifficulty?.ToString() ?? DBNull.Value, NpgsqlTypes.NpgsqlDbType.Varchar);
                    }
                    writer.Complete();
                    conn.Close();
                }   
        }
    }
}