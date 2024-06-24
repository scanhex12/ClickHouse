#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function test_login_pwd
{
  ${CLICKHOUSE_CLIENT} --user $1 --password $2 --query "select 1"
}

function test_login_pwd_expect_error
{
  test_login_pwd "$1" "$2" 2>&1 | grep -m1 -o 'AUTHENTICATION_FAILED'
}

user="u01_03174"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY '1'"

echo "Basic authentication after user creation"
test_login_pwd ${user} '1'

${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} IDENTIFIED WITH plaintext_password BY '2'"

echo "Changed password, old password should not work"
test_login_pwd_expect_error ${user} '1'

echo "New password should work"
test_login_pwd ${user} '2'

${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY '3' ADD IDENTIFIED WITH plaintext_password BY '4'"

echo "Two new passwords were added, should both work"
test_login_pwd ${user} '3'

test_login_pwd ${user} '4'

echo "Altering credentials and keeping only bcrypt_password"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} IDENTIFIED WITH bcrypt_password BY '5'"

echo "Asserting bcrypt_password works"
test_login_pwd ${user} '5'

echo "Adding new bcrypt_password"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} ADD IDENTIFIED WITH bcrypt_password BY '6'"

echo "Both current authentication methods should work"
test_login_pwd ${user} '5'
test_login_pwd ${user} '6'

echo "Reset authentication methods to new"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} RESET AUTHENTICATION METHODS TO NEW"

echo "Only the latest should work, below should fail"
test_login_pwd_expect_error ${user} '5'

echo "Should work"
test_login_pwd ${user} '6'

echo "Multiple identified with, not allowed"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} IDENTIFIED WITH plaintext_password '7' IDENTIFIED WITH plaintext_password '8'" 2>&1 | grep -m1 -o "SYNTAX_ERROR"
echo "Multiple identified with, not allowed, even if mixed"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} IDENTIFIED WITH plaintext_password '7' ADD IDENTIFIED WITH plaintext_password '8' IDENTIFIED WITH plaintext_password '9'" 2>&1 | grep -m1 -o "SYNTAX_ERROR"
echo "Identified with must precede all add identified with, not allowed"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password '7' IDENTIFIED WITH plaintext_password '8'" 2>&1 | grep -m1 -o "SYNTAX_ERROR"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"

echo "CREATE Multiple identified with, not allowed"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password '7' IDENTIFIED WITH plaintext_password '8'" 2>&1 | grep -m1 -o "SYNTAX_ERROR"
echo "CREATE Multiple identified with, not allowed, even if mixed"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password '7' ADD IDENTIFIED WITH plaintext_password '8' IDENTIFIED WITH plaintext_password '9'" 2>&1 | grep -m1 -o "SYNTAX_ERROR"
echo "CREATE Identified with must precede all add identified with, not allowed"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} ADD IDENTIFIED WITH plaintext_password '7' IDENTIFIED WITH plaintext_password '8'" 2>&1 | grep -m1 -o "SYNTAX_ERROR"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
