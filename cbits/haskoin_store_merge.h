/*
 * The merge operators of haskoin-store: the C functions that RocksDB calls
 * to work out an address balance and a group of spenders from the merge
 * records that the import of a block writes.
 *
 * Why: importing a block changes the balance of every address that it pays
 * or spends from, and adds a spender to the group of every output that it
 * spends. Each of those was read from the database, changed and written
 * back, and on a store of Bitcoin those reads were most of the time of a
 * block. A merge record says how a value changes and is written without
 * reading it. RocksDB works out the value later, when it is read or when a
 * compaction brings the records and the value together, with the functions
 * below. They run on the threads of RocksDB, so they are C.
 *
 * Both formats are made so that a merge record looks like a value. Then
 * applying records to a value and combining records with each other are
 * the same operation, and a record that is merged with no value is the
 * value.
 *
 * The functions have the types that rocksdb_mergeoperator_create of
 * rocksdb/c.h asks for. They return memory from malloc(), which RocksDB
 * frees. When their input is not of the format they fail: they store 0 in
 * "*success" and in "*new_value_length" and return NULL. They do nothing
 * else about such input.
 *
 * Haskoin.Store.Database.Merge gives them to RocksDB, and
 * docs/layered-database.md has the reasons.
 */

#ifndef HASKOIN_STORE_MERGE_H
#define HASKOIN_STORE_MERGE_H

#include <stddef.h>

/* Bytes of an address balance: five numbers of eight bytes. */
#define HASKOIN_STORE_BALANCE_SIZE 40

/*
 * Bytes of an entry of a group of spenders: the number of the output in
 * its transaction (4), the hash of the transaction that spends it (32) and
 * the number of the input that does (4).
 */
#define HASKOIN_STORE_SPENDER_SIZE 40

/*
 * Address balances: the values of the "balance" column family.
 *
 * A value is five unsigned numbers of 64 bits, most significant byte
 * first: the confirmed balance, the unconfirmed balance, the number of
 * unspent outputs, the number of transactions, and the amount received. It
 * is what the store has always written (BalVal).
 *
 * A merge record has the same forty bytes: five differences, each to be
 * added to its number modulo 2^64, so that a number goes down by adding
 * its two's complement. A key without a value counts from five zeros.
 *
 * The full merge adds the records to the value, and the partial merge adds
 * records to each other. Addition modulo 2^64 does not care about order or
 * grouping, so both are the same sum.
 */
char *haskoin_store_balance_full_merge(
    void *state,
    const char *key,
    size_t key_length,
    const char *existing_value,
    size_t existing_value_length,
    const char *const *operands_list,
    const size_t *operands_list_length,
    int num_operands,
    unsigned char *success,
    size_t *new_value_length);

char *haskoin_store_balance_partial_merge(
    void *state,
    const char *key,
    size_t key_length,
    const char *const *operands_list,
    const size_t *operands_list_length,
    int num_operands,
    unsigned char *success,
    size_t *new_value_length);

/*
 * Groups of spenders: the values of the "spender" column family.
 *
 * A value is one byte with the number of its entries, then the entries in
 * ascending order of their first field, each of
 * HASKOIN_STORE_SPENDER_SIZE bytes: the number of an output in its
 * transaction, the hash of the transaction that spends it, and the number
 * of the input that does, the two numbers of 32 bits with the most
 * significant byte first. It is what the store has written since database
 * version 22 (SpenderVal).
 *
 * A merge record is a value of the same format, with the entries to add.
 * Merging is the union: every entry of the value and of the records, and
 * for an output that several have, the entry of the newest. The result is
 * in ascending order of output again.
 *
 * The full merge and the partial merge are the same union, the first with
 * the value as the oldest of what it is given.
 */
char *haskoin_store_spender_full_merge(
    void *state,
    const char *key,
    size_t key_length,
    const char *existing_value,
    size_t existing_value_length,
    const char *const *operands_list,
    const size_t *operands_list_length,
    int num_operands,
    unsigned char *success,
    size_t *new_value_length);

char *haskoin_store_spender_partial_merge(
    void *state,
    const char *key,
    size_t key_length,
    const char *const *operands_list,
    const size_t *operands_list_length,
    int num_operands,
    unsigned char *success,
    size_t *new_value_length);

#endif
