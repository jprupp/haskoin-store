/*
 * The merge operators of haskoin-store. See haskoin_store_merge.h for what
 * each function is for and the formats that it reads and writes.
 */

#include "haskoin_store_merge.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* The numbers of a balance. */
#define BALANCE_FIELDS 5

/* The most entries that the byte in front of a group can count. */
#define MOST_SPENDERS 255

/* Say that a merge failed, the way RocksDB's C API wants it said. */
static char *fail(unsigned char *success, size_t *new_value_length)
{
    *success = 0;
    *new_value_length = 0;
    return NULL;
}

/* A number of 64 bits, most significant byte first. */
static uint64_t read_be64(const unsigned char *bytes)
{
    uint64_t number = 0;
    int i;

    for (i = 0; i < 8; i++) {
        number = (number << 8) | bytes[i];
    }
    return number;
}

static void write_be64(unsigned char *bytes, uint64_t number)
{
    int i;

    for (i = 7; i >= 0; i--) {
        bytes[i] = (unsigned char)(number & 0xff);
        number >>= 8;
    }
}

/*
 * Add the five numbers of a balance, or of a merge record, to five sums.
 * Returns zero if it is not of forty bytes.
 */
static int add_balance(uint64_t *sums, const char *bytes, size_t length)
{
    int i;

    if (length != HASKOIN_STORE_BALANCE_SIZE) {
        return 0;
    }
    for (i = 0; i < BALANCE_FIELDS; i++) {
        /* unsigned, so that it wraps around at 2^64 */
        sums[i] += read_be64((const unsigned char *)bytes + 8 * i);
    }
    return 1;
}

/*
 * The sum of a first balance, if there is one, and of some more.
 */
static char *sum_balances(
    const char *first,
    size_t first_length,
    const char *const *list,
    const size_t *lengths,
    int count,
    unsigned char *success,
    size_t *new_value_length)
{
    uint64_t sums[BALANCE_FIELDS] = {0, 0, 0, 0, 0};
    unsigned char *sum;
    int i;

    if (first != NULL && !add_balance(sums, first, first_length)) {
        return fail(success, new_value_length);
    }
    for (i = 0; i < count; i++) {
        if (!add_balance(sums, list[i], lengths[i])) {
            return fail(success, new_value_length);
        }
    }
    sum = malloc(HASKOIN_STORE_BALANCE_SIZE);
    if (sum == NULL) {
        return fail(success, new_value_length);
    }
    for (i = 0; i < BALANCE_FIELDS; i++) {
        write_be64(sum + 8 * i, sums[i]);
    }
    *success = 1;
    *new_value_length = HASKOIN_STORE_BALANCE_SIZE;
    return (char *)sum;
}

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
    size_t *new_value_length)
{
    (void)state;
    (void)key;
    (void)key_length;
    return sum_balances(
        existing_value,
        existing_value_length,
        operands_list,
        operands_list_length,
        num_operands,
        success,
        new_value_length);
}

char *haskoin_store_balance_partial_merge(
    void *state,
    const char *key,
    size_t key_length,
    const char *const *operands_list,
    const size_t *operands_list_length,
    int num_operands,
    unsigned char *success,
    size_t *new_value_length)
{
    (void)state;
    (void)key;
    (void)key_length;
    return sum_balances(NULL, 0, operands_list, operands_list_length, num_operands, success, new_value_length);
}

/*
 * The number of entries that a group says it has, or -1 if its length is
 * not that of so many entries after the byte that counts them.
 */
static int spender_count(const char *group, size_t length)
{
    size_t count;

    if (length < 1) {
        return -1;
    }
    count = (unsigned char)group[0];
    if (length != 1 + count * HASKOIN_STORE_SPENDER_SIZE) {
        return -1;
    }
    return (int)count;
}

/*
 * Put the entries of a group into a list of "*used" entries that is in
 * ascending order of output, each where its output belongs, and in place of
 * the entry that the list has for that output if it has one. The list has
 * room for every entry that it will be given.
 *
 * The number of the output is the first four bytes of an entry, most
 * significant first, so the order of the outputs is the order of those
 * bytes.
 */
static void add_spenders(unsigned char *entries, size_t *used, const char *group, int count)
{
    const size_t size = HASKOIN_STORE_SPENDER_SIZE;
    int i;

    for (i = 0; i < count; i++) {
        const char *entry = group + 1 + (size_t)i * size;
        size_t at = 0;
        int order = 1;

        /* groups are of sixteen outputs: a walk is as good as a search */
        while (at < *used) {
            order = memcmp(entry, entries + at * size, 4);
            if (order <= 0) {
                break;
            }
            at += 1;
        }
        if (at == *used || order < 0) {
            /* a new output: make room for it */
            memmove(entries + (at + 1) * size, entries + at * size, (*used - at) * size);
            *used += 1;
        }
        memcpy(entries + at * size, entry, size);
    }
}

/*
 * The union of a first group, if there is one, and of some more, from the
 * oldest to the newest.
 */
static char *join_spenders(
    const char *first,
    size_t first_length,
    const char *const *list,
    const size_t *lengths,
    int count,
    unsigned char *success,
    size_t *new_value_length)
{
    const size_t size = HASKOIN_STORE_SPENDER_SIZE;
    size_t most = 0;
    size_t used = 0;
    unsigned char *joined;
    int entries;
    int i;

    /* every group is checked before anything is read from one */
    if (first != NULL) {
        entries = spender_count(first, first_length);
        if (entries < 0) {
            return fail(success, new_value_length);
        }
        most += (size_t)entries;
    }
    for (i = 0; i < count; i++) {
        entries = spender_count(list[i], lengths[i]);
        if (entries < 0) {
            return fail(success, new_value_length);
        }
        most += (size_t)entries;
    }

    /* the byte that counts, and room for as many entries as there can be */
    joined = malloc(1 + most * size);
    if (joined == NULL) {
        return fail(success, new_value_length);
    }
    if (first != NULL) {
        add_spenders(joined + 1, &used, first, spender_count(first, first_length));
    }
    for (i = 0; i < count; i++) {
        add_spenders(joined + 1, &used, list[i], spender_count(list[i], lengths[i]));
    }
    if (used > MOST_SPENDERS) {
        free(joined);
        return fail(success, new_value_length);
    }
    joined[0] = (unsigned char)used;
    *success = 1;
    *new_value_length = 1 + used * size;
    return (char *)joined;
}

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
    size_t *new_value_length)
{
    (void)state;
    (void)key;
    (void)key_length;
    return join_spenders(
        existing_value,
        existing_value_length,
        operands_list,
        operands_list_length,
        num_operands,
        success,
        new_value_length);
}

char *haskoin_store_spender_partial_merge(
    void *state,
    const char *key,
    size_t key_length,
    const char *const *operands_list,
    const size_t *operands_list_length,
    int num_operands,
    unsigned char *success,
    size_t *new_value_length)
{
    (void)state;
    (void)key;
    (void)key_length;
    return join_spenders(NULL, 0, operands_list, operands_list_length, num_operands, success, new_value_length);
}
