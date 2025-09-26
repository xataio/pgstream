# 🔀 Transformers

![transformer diagram](img/pgstream_transformer_diagram.svg)

pgstream supports column value transformations to anonymize or mask sensitive data during replication and snapshots. This is particularly useful for compliance with data privacy regulations.

pgstream integrates with existing transformer open source libraries, such as [greenmask](https://github.com/GreenmaskIO/greenmask), [neosync](https://github.com/nucleuscloud/neosync) and [go-masker](https://github.com/ggwhite/go-masker), to leverage a large amount of transformation capabilities, as well as having support for custom transformations.

## Supported transformers

### PostgreSQL Anonymizer

 <details>
  <summary>pg_anonymizer</summary>

**Description:** Integrates with the [PostgreSQL Anonymizer](https://postgresql-anonymizer.readthedocs.io/en/stable/) extension to provide advanced data anonymization using built-in anonymizer functions.

⚠️ This transformer requires a PostgreSQL database connection to execute transformations, which may impact performance compared to other transformers in this document that generate values locally without database queries.

| Supported PostgreSQL types                                                                                    |
| ------------------------------------------------------------------------------------------------------------- |
| Dependent on [anonymizer function](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/) |

| Parameter         | Type   | Default                      | Required | Values                                                                                                                                                                           |
| ----------------- | ------ | ---------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| anon_function     | string | N/A                          | Yes      | Any valid anon.\* [function](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/). If no parameters are required, it can be provided with or without `()`. |
| postgres_url      | string | pgsteram source Postgres URL | Yes      | PostgreSQL connection URL where the function is run. Defaults to pgstream source URL when configured.                                                                            |
| salt              | string | ""                           | No       | Salt for `anon.pseudo_` and `anon.digest` functions                                                                                                                              |
| hash_algorithm    | string | sha256                       | No       | Hash algorithm for `anon.digest`. One of md5, sha224, sha256, sha384, sha512                                                                                                     |
| interval          | string | N/A                          | No       | Time interval for `anon.dnoise` function                                                                                                                                         |
| ratio             | string | N/A                          | No       | Noise ratio for `anon.noise` function                                                                                                                                            |
| sigma             | string | N/A                          | No       | Blur sigma for `anon.image_blur` function                                                                                                                                        |
| mask              | string | N/A                          | No       | Mask character for `anon.partial` function                                                                                                                                       |
| mask_prefix_count | int    | 0                            | No       | Prefix count for `anon.partial` function                                                                                                                                         |
| mask_suffix_count | int    | 0                            | No       | Suffix count for `anon.partial` function                                                                                                                                         |

Notes:

- The transformer executes functions directly in PostgreSQL, ensuring compatibility with all anonymizer features
- Deterministic functions (pseudo\_\*, hash, digest) produce consistent output for the same input
- Functions that don't require parameters (like anon.fake\_\*()) can be used without additional configuration

**Prerequisites:**

- PostgreSQL Anonymizer extension must be installed and enabled on the source (or the configured url)
- Extension must be loaded in `shared_preload_libraries`
- Run `SELECT anon.init();`in order to use the faking functions

**Supported Functions:**

- [Adding noise](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/#adding-noise)
- [Randomization](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/#randomization)
- [Faking](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/#faking)
- [Advanced faking](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/#advanced-faking)
- [Pseudoanonymization](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/#pseudonymization)
- [Generic hashing](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/#generic-hashing)
- [Partial scrambling](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/#partial-scrambling)
- [Image blurring](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/#image-bluring)

**Unsupported Functions:**

- [Conditional masking](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/#conditional-masking)
- [Generalization](https://postgresql-anonymizer.readthedocs.io/en/stable/masking_functions/#generalization)

**Example Configurations:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        first_name:
          name: pg_anonymizer
          parameters:
            anon_function: anon.fake_first_name()
        id:
          name: pg_anonymizer
          parameters:
            anon_function: anon.digest
            salt: salt
            hash_algorithm: md5
```

**Input-Output Examples:**

| Input Value        | Function Configuration                                                               | Output Value                          |
| ------------------ | ------------------------------------------------------------------------------------ | ------------------------------------- |
| `John`             | `anon_function: anon.fake_first_name()`                                              | `Michael` (random)                    |
| `john@test.com`    | `anon_function: anon.pseudo_email, salt: "key123"`                                   | `alice@test.com` (deterministic)      |
| `1234567890`       | `anon_function: anon.partial, mask: "*", mask_prefix_count: 3, mask_suffix_count: 3` | `123****890`                          |
| `sensitive_data`   | `anon_function: anon.digest, salt: "key", hash_algorithm: "sha256"`                  | `a1b2c3d4e5f6...` (hash)              |
| `100.50`           | `anon_function: anon.noise, ratio: "0.1"`                                            | `95.23` (with 10% noise)              |
| `2023-01-15`       | `anon_function: anon.dnoise, interval: "1 day"`                                      | `2023-01-16` (±1 day noise)           |
| `password123`      | `anon_function: anon.hash`                                                           | `ef92b778bafe771e89245b89ecbc08a4...` |
| `Alice Smith`      | `anon_function: anon.pseudo_first_name, salt: "s1"`                                  | `Bob Smith` (deterministic)           |
| `user@company.com` | `anon_function: anon.partial_email`                                                  | `u***@company.com`                    |
| `42`               | `anon_function: anon.random_int_between(1, 100)`                                     | `73` (random between 1-100)           |
| `/path/image.jpg`  | `anon_function: anon.image_blur, sigma: "2.5"`                                       | Blurred image data                    |
| Any value          | `anon_function: anon.fake_company()`                                                 | `Acme Corporation` (random)           |

</details>

### Greenmask

 <details>
  <summary>greenmask_boolean</summary>

**Description:** Generates random or deterministic boolean values (`true` or `false`).

| Supported PostgreSQL types |
| -------------------------- |
| `boolean`                  |

| Parameter | Type   | Default | Required | Values               |
| --------- | ------ | ------- | -------- | -------------------- |
| generator | string | random  | No       | random,deterministic |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        is_active:
          name: greenmask_boolean
          parameters:
            generator: deterministic
```

**Input-Output Examples:**

| Input Value | Configuration Parameters   | Output Value               |
| ----------- | -------------------------- | -------------------------- |
| `true`      | `generator: deterministic` | `false`                    |
| `false`     | `generator: deterministic` | `true`                     |
| `true`      | `generator: random`        | `true` or `false` (random) |

</details>

 <details>
  <summary>greenmask_choice</summary>

**Description:** Randomly selects a value from a predefined list of choices.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter | Type     | Default | Required | Values               |
| --------- | -------- | ------- | -------- | -------------------- |
| generator | string   | random  | No       | random,deterministic |
| choices   | string[] | N/A     | Yes      | N/A                  |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: orders
      column_transformers:
        status:
          name: greenmask_choice
          parameters:
            generator: random
            choices: ["pending", "shipped", "delivered", "cancelled"]
```

**Input-Output Examples:**

| Input Value | Configuration Parameters   | Output Value         |
| ----------- | -------------------------- | -------------------- |
| `pending`   | `generator: random`        | `shipped` (random)   |
| `shipped`   | `generator: deterministic` | `pending`            |
| `delivered` | `generator: random`        | `cancelled` (random) |

</details>

 <details>
  <summary>greenmask_date</summary>

**Description:** Generates random or deterministic dates within a specified range.

| Supported PostgreSQL types         |
| ---------------------------------- |
| `date`, `timestamp`, `timestamptz` |

| Parameter | Type                  | Default | Required | Values               |
| --------- | --------------------- | ------- | -------- | -------------------- |
| generator | string                | random  | No       | random,deterministic |
| min_value | string (`yyyy-MM-dd`) | N/A     | Yes      | N/A                  |
| max_value | string (`yyyy-MM-dd`) | N/A     | Yes      | N/A                  |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: events
      column_transformers:
        event_date:
          name: greenmask_date
          parameters:
            generator: random
            min_value: "2020-01-01"
            max_value: "2025-12-31"
```

**Input-Output Examples:**

| Input Value  | Configuration Parameters                                          | Output Value          |
| ------------ | ----------------------------------------------------------------- | --------------------- |
| `2023-01-01` | `generator: random, min_value: 2020-01-01, max_value: 2025-12-31` | `2021-05-15` (random) |
| `2022-06-15` | `generator: deterministic`                                        | `2020-01-01`          |

</details>

 <details>
  <summary>greenmask_firstname</summary>

**Description:** Generates random or deterministic first names, optionally filtered by gender.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter | Type   | Default | Required | Values               | Dynamic |
| --------- | ------ | ------- | -------- | -------------------- | ------- |
| generator | string | random  | No       | random,deterministic | No      |
| gender    | string | Any     | No       | Any,Female,Male      | Yes     |

`gender` can also be a dynamic parameter, referring to some other column. Please see the below example config.

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: employees
      column_transformers:
        first_name:
          name: greenmask_firstname
          parameters:
            generator: deterministic
          dynamic_parameters:
            gender:
              column: sex
```

**Input-Output Examples:**

| Input Name | Configuration Parameters | Output Name |
| ---------- | ------------------------ | ----------- |
| `John`     | `preserve_gender: true`  | `Michael`   |
| `Jane`     | `preserve_gender: true`  | `Emily`     |
| `Alex`     | `preserve_gender: false` | `Jordan`    |
| `Chris`    | `generator: random`      | `Taylor`    |

</details>

 <details>
  <summary>greenmask_float</summary>

**Description:** Generates random or deterministic floating-point numbers within a specified range.

| Supported PostgreSQL types |
| -------------------------- |
| `real`, `double precision` |

| Parameter | Type   | Default                                       | Required | Values               |
| --------- | ------ | --------------------------------------------- | -------- | -------------------- |
| generator | string | random                                        | No       | random,deterministic |
| min_value | float  | -3.40282346638528859811704183484516925440e+38 | No       | N/A                  |
| max_value | float  | 3.40282346638528859811704183484516925440e+38  | No       | N/A                  |

</details>

 <details>
  <summary>greenmask_integer</summary>

**Description:** Generates random or deterministic integers within a specified range.

| Supported PostgreSQL types     |
| ------------------------------ |
| `smallint`,`integer`, `bigint` |

| Parameter | Type   | Default     | Required | Values               |
| --------- | ------ | ----------- | -------- | -------------------- |
| generator | string | random      | No       | random,deterministic |
| size      | int    | 4           | No       | 2,4                  |
| min_value | int    | -2147483648 | No       | N/A                  |
| max_value | int    | 2147483647  | No       | N/A                  |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: products
      column_transformers:
        stock_quantity:
          name: greenmask_integer
          parameters:
            generator: random
            min_value: 1
            max_value: 1000
```

</details>

 <details>
  <summary>greenmask_string</summary>

**Description:** Generates random or deterministic strings with customizable length and character set.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter  | Type   | Default                                                        | Required | Values               |
| ---------- | ------ | -------------------------------------------------------------- | -------- | -------------------- |
| generator  | string | random                                                         | No       | random,deterministic |
| symbols    | string | abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 | No       | N/A                  |
| min_length | int    | 1                                                              | No       | N/A                  |
| max_length | int    | 100                                                            | No       | N/A                  |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        username:
          name: greenmask_string
          parameters:
            generator: random
            min_length: 5
            max_length: 15
            symbols: "abcdefghijklmnopqrstuvwxyz1234567890"
```

</details>

 <details>
  <summary>greenmask_unix_timestamp</summary>

**Description:** Generates random or deterministic unix timestamps.

| Supported PostgreSQL types    |
| ----------------------------- |
| `smallint`,`integer`,`bigint` |

| Parameter | Type   | Default | Required | Values               |
| --------- | ------ | ------- | -------- | -------------------- |
| generator | string | random  | No       | random,deterministic |
| min_value | string | N/A     | Yes      | N/A                  |
| max_value | string | N/A     | Yes      | N/A                  |

</details>

 <details>
  <summary>greenmask_utc_timestamp</summary>

**Description:** Generates random or deterministic UTC timestamps.

| Supported PostgreSQL types |
| -------------------------- |
| `timestamp`                |

| Parameter     | Type               | Default | Required | Values                                                               |
| ------------- | ------------------ | ------- | -------- | -------------------------------------------------------------------- |
| generator     | string             | random  | No       | random,deterministic                                                 |
| truncate_part | string             | ""      | No       | nanosecond,microsecond,millisecond,second,minute,hour,day,month,year |
| min_timestamp | string (`RFC3339`) | N/A     | Yes      | N/A                                                                  |
| max_timestamp | string (`RFC3339`) | N/A     | Yes      | N/A                                                                  |

</details>

 <details>
  <summary>greenmask_uuid</summary>

**Description:** Generates random or deterministic UUIDs.

| Supported PostgreSQL types                 |
| ------------------------------------------ |
| `uuid`,`text`, `varchar`, `char`, `bpchar` |

| Parameter | Type   | Default | Required | Values               |
| --------- | ------ | ------- | -------- | -------------------- |
| generator | string | random  | No       | random,deterministic |

</details>

### Neosync

</details>
 <details>
  <summary>neosync_email</summary>

**Description:** Anonymizes email addresses while optionally preserving length and domain.

| Supported PostgreSQL types                    |
| --------------------------------------------- |
| `text`, `varchar`, `char`, `bpchar`, `citext` |

| Parameter            | Type     | Default | Required | Values                           |
| -------------------- | -------- | ------- | -------- | -------------------------------- |
| preserve_length      | bool     | false   | No       |                                  |
| preserve_domain      | bool     | false   | No       |                                  |
| excluded_domains     | string[] | N/A     | No       |                                  |
| max_length           | int      | 100     | No       |                                  |
| email_type           | string   | uuidv4  | No       | uuidv4,fullname,any              |
| invalid_email_action | string   | 100     | No       | reject,passthrough,null,generate |
| seed                 | int      | Rand    | No       |                                  |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: customers
      column_transformers:
        email:
          name: neosync_email
          parameters:
            preserve_length: true
            preserve_domain: true
```

**Input-Output Examples:**

| Input Email            | Configuration Parameters                        | Output Email           |
| ---------------------- | ----------------------------------------------- | ---------------------- |
| `john.doe@example.com` | `preserve_length: true, preserve_domain: true`  | `abcd.efg@example.com` |
| `jane.doe@company.org` | `preserve_length: false, preserve_domain: true` | `random@company.org`   |
| `user123@gmail.com`    | `preserve_length: true, preserve_domain: false` | `abcde123@random.com`  |
| `invalid-email`        | `invalid_email_action: passthrough`             | `invalid-email`        |
| `invalid-email`        | `invalid_email_action: null`                    | `NULL`                 |
| `invalid-email`        | `invalid_email_action: generate`                | `generated@random.com` |

</details>

 <details>
  <summary>neosync_firstname</summary>

**Description:** Generates anonymized first names while optionally preserving length.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter       | Type | Default | Required |
| --------------- | ---- | ------- | -------- |
| preserve_length | bool | false   | No       |
| max_length      | int  | 100     | No       |
| seed            | int  | Rand    | No       |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        first_name:
          name: neosync_firstname
          parameters:
            preserve_length: true
```

</details>
 <details>
  <summary>neosync_lastname</summary>

**Description:** Generates anonymized last names while optionally preserving length.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter       | Type | Default | Required |
| --------------- | ---- | ------- | -------- |
| preserve_length | bool | false   | No       |
| max_length      | int  | 100     | No       |
| seed            | int  | Rand    | No       |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        last_name:
          name: neosync_lastname
          parameters:
            preserve_length: true
```

</details>
 <details>
  <summary>neosync_fullname</summary>

**Description:** Generates anonymized full names while optionally preserving length.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter       | Type | Default | Required |
| --------------- | ---- | ------- | -------- |
| preserve_length | bool | false   | No       |
| max_length      | int  | 100     | No       |
| seed            | int  | Rand    | No       |

max_length must be greater than 2. If preserve_length is set to true, generated value can might be longer than max_length, depending on the input length.

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        full_name:
          name: neosync_fullname
          parameters:
            preserve_length: true
```

</details>
 <details>
  <summary>neosync_string</summary>

**Description:** Generates anonymized strings with customizable length.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter       | Type | Default | Required |
| --------------- | ---- | ------- | -------- |
| preserve_length | bool | false   | No       |
| min_length      | int  | 1       | No       |
| max_length      | int  | 100     | No       |
| seed            | int  | Rand    | No       |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: logs
      column_transformers:
        log_message:
          name: neosync_string
          parameters:
            min_length: 10
            max_length: 50
```

</details>

### Xata

 <details>
  <summary>template</summary>

**Description:** Transforms the data using go templates

| Supported PostgreSQL types             |
| -------------------------------------- |
| All types with a string representation |

| Parameter | Type   | Default | Required |
| --------- | ------ | ------- | -------- |
| template  | string | N/A     | Yes      |

This transformer can be used for any Postgres type as long as the given template produces a value with correct syntax for that column type. e.g It can be "5-10-2021" for a date column, or "3.14159265" for a double precision one.

Template transformer supports a bunch of useful functions. Use `.GetValue` to refer to the value to be transformed. Use `.GetDynamicValue "<column_name>"` to refer to some other column. Other than the standard go template functions, there are many useful helper functions supported to be used with template transformer, thanks to `greenmask`'s huge set of [core functions](https://docs.greenmask.io/latest/built_in_transformers/advanced_transformers/custom_functions/core_functions/) including `masking` function by `go-masker` and various [random data generator functions](https://docs.greenmask.io/latest/built_in_transformers/advanced_transformers/custom_functions/faker_function/) powered by the open source library `faker`. Also, template transformer has support for the open source library [sprig](https://masterminds.github.io/sprig/) which has many useful helper functions.

With the below example config `pgstream` masks values in the column `email` of the table `users`, using `go-masker`'s email masking function. But first, this template checks if there's a non-empty value to be used in the column `email`. If not, it simply looks for another column named `secondary_email` and uses that instead. Then we have another check to see if it's a `@xata` email or not. Finally masking the value, only if it's not a `@xata` email, passing it without a mask otherwise.

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: template
          parameters:
            template: >
              {{ $email := "" }}
              {{- if and (ne .GetValue nil) (isString .GetValue) (gt (len .GetValue) 0) -}}
                {{ $email = .GetValue }}
              {{- else -}}
                {{ $email = .GetDynamicValue "secondary_email" }}
              {{- end -}}
              {{- if (not (contains "@xata" $email)) -}}
                {{ masking "email" $email }}
              {{- else -}}
                {{ $email }}
              {{- end -}}
```

</details>

 <details>
  <summary>masking</summary>

**Description:** Masks string values using the provided masking function.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

**Parameter Details:**

| Parameter | Type   | Default | Required | Values                                                                             |
| --------- | ------ | ------- | -------- | ---------------------------------------------------------------------------------- |
| type      | string | default | No       | custom, password, name, address, email, mobile, tel, id, credit_card, url, default |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: masking
          parameters:
            type: email
```

**Input-Output Examples:**

| Input Value              | Configuration Parameters | Output Value           |
| ------------------------ | ------------------------ | ---------------------- |
| `aVeryStrongPassword123` | `type: password`         | `************`         |
| `john.doe@example.com`   | `type: email`            | `joh****e@example.com` |
| `Sensitive Data`         | `type: default`          | `**************`       |

With `custom` type, the masking function is defined by the user, by providing beginning and end indexes for masking. If the input is shorter than the end index, the rest of the string will all be masked. See the third example below.

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: masking
          parameters:
            type: custom
            mask_begin: "4"
            mask_end: "12"
```

**Input-Output Examples:**

| Input Value             | Output Value            |
| ----------------------- | ----------------------- |
| `1234567812345678`      | `1234********5678`      |
| `sensitive@example.com` | `sens********ample.com` |
| `sensitive`             | `sens*****`             |

If the begin index is not provided, it defaults to 0. If the end is not provided, it defaults to input length.

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: masking
          parameters:
            type: custom
            mask_end: "5"
```

| Input Value             | Output Value            |
| ----------------------- | ----------------------- |
| `1234567812345678`      | `*****67812345678`      |
| `sensitive@example.com` | `*****tive@example.com` |
| `sensitive`             | `*****tive`             |

Alternatively, since input length may vary, user can provide relative beginning and end indexes, as percentages of the input length.

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: masking
          parameters:
            type: custom
            mask_begin: "15%"
            mask_end: "85%"
```

| Input Value             | Output Value            |
| ----------------------- | ----------------------- |
| `1234567812345678`      | `12***********678`      |
| `sensitive@example.com` | `sen***************com` |
| `sensitive`             | `s******ve`             |

Alternatively, user can provide unmask begin and end indexes. In that case, the specified part of the input will remain unmasked, while all the rest is masked.
Mask and unmask parameters cannot be provided at the same time.

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: masking
          parameters:
            type: custom
            unmask_end: "3"
```

| Input Value             | Output Value            |
| ----------------------- | ----------------------- |
| `1234567812345678`      | `123*************`      |
| `sensitive@example.com` | `sen******************` |
| `sensitive`             | `sen******`             |

</details>

 <details>
  <summary>json</summary>

**Description:** Transforms json data with set and delete operations

| Supported PostgreSQL types |
| -------------------------- |
| json, jsonb                |

| Parameter  | Type  | Default | Required |
| ---------- | ----- | ------- | -------- |
| operations | array | N/A     | Yes      |

Parameter for each operation:
| Parameter | Type | Default | Required | Values |
| --------------- | ------- | ------- | -------- | ------------------------------ |
| operation | string | N/A | Yes | set, delete |
| path | string | N/A | Yes | sjson syntax<sup>\*</sup> |
| skip_not_exist | boolean | true | No | true, false |
| error_not_exist | boolean | false | No | true, false |
| value | string | N/A | Yes<sup>\*\*</sup>| Any valid JSON representation |
| value_template | string | N/A | Yes<sup>\*\*</sup> | Any template with valid syntax |

<sup>\*</sup>_Paths should follow [sjson syntax](https://github.com/tidwall/sjson#path-syntax)_

<sup>\*\*</sup>_Either `value` or `value_template` must be provided if the operation is `set`. If both are provided, `value_template` takes precedence._

JSON transformer can be used for Postgres types json and jsonb. This transformer executes a list of given operations on the json data to be transformed.

All operations must be either `set` or `delete`.
`set` operations support literal values as well as templates, making use of `sprig` and `greenmask`'s function sets. See template transformer section for more details. Also, like the template transformer, `.GetValue` and `.GetDynamicValue` functions are supported. Unlike template transformer, here `.GetValue` refers to the value at given path, rather than the entire JSON object; whereas `.GetDynamicValue` is again used for referring to other columns.
`delete` operations simply delete the object at the given path.
Execution of an operation will be skipped if the given path does not exists and the parameter `skip_not_exist` is set to true, which is also the default behavior.
Execution of an operation errors out if the given path does not exists and the parameter `error_not_exist` - which is false by default - is set to true; unless the operation is skipped already.

JSON transformer uses [sjson](https://github.com/tidwall/sjson) library for executing the operations. Operation paths should follow the [synxtax rules of sjson](https://github.com/tidwall/sjson#path-syntax)

With the below config `pgstream` transforms the json values in the column `user_info_json` of the table `users` by:

- First, traversing all the items in the array named `purchases`, and for each element, setting value to "-" for key "item".
- Then, deleting the object named "country" under the top-level object "address".
- Completely masking the "city" value under object "address", using `go-masker`'s default masking function supported by `pgstream`'s templating.
- Finally, setting the user's lastname after fetching it from some other column named `lastname`, using dynamic values support. Assuming there's such column, having the lastname info for users.

Example input-output is given below the config.

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        user_info_json:
          name: json
          parameters:
            operations:
              - operation: set
                path: "purchases.#.item"
                value: "-"
                error_not_exist: true
              - operation: delete
                path: "address.country"
              - operation: set
                path: "address.city"
                value_template: '"{{ masking "default" .GetValue }}"'
              - operation: set
                path: "user.lastname"
                value_template: '"{{ .GetDynamicValue "lastname" }}"'
```

For input JSON value,

```json
{
  "user": {
    "firstname": "john",
    "lastname": "unknown"
  },
  "residency": {
    "city": "some city",
    "country": "some country"
  },
  "purchases": [
    {
      "item": "book",
      "price": 10
    },
    {
      "item": "pen",
      "price": 2
    }
  ]
}
```

the JSON transformer with above config produces output:

```json
{
  "user": {
    "firstname": "john",
    "lastname": "doe"
  },
  "residency": {
    "city": "*********"
  },
  "purchases": [
    {
      "item": "-",
      "price": 10
    },
    {
      "item": "-",
      "price": 2
    }
  ]
}
```

</details>

 <details>
  <summary>hstore</summary>

**Description:** Transforms hstore data with set and delete operations

| Supported PostgreSQL types |
| -------------------------- |
| hstore                     |

| Parameter  | Type  | Default | Required |
| ---------- | ----- | ------- | -------- |
| operations | array | N/A     | Yes      |

Parameter for each operation:
| Parameter | Type | Default | Required | Values |
| --------------- | ------- | ------- | -------- | ------------------------------ |
| operation | string | N/A | Yes | set, delete |
| key | string | N/A | Yes | |
| skip_not_exist | boolean | true | No | true, false |
| error_not_exist | boolean | false | No | true, false |
| value | string, null | N/A | Yes<sup>\*</sup> | Any string or null |
| value_template | string | N/A | Yes<sup>\*</sup> | Any template with valid syntax |

<sup>\*</sup>_Either `value` or `value_template` must be provided if the operation is `set`. If both are provided, `value_template` takes precedence._

Hstore transformer can be used for Postgres type hstore. This transformer executes a list of given operations on the hstore data to be transformed.

All operations must be either `set` or `delete`.
`set` operations support literal values as well as templates, making use of `sprig` and `greenmask`'s function sets. See template transformer section for more details. Also, like the template transformer, `.GetValue` and `.GetDynamicValue` functions are supported. Unlike template transformer, here `.GetValue` refers to the value for the given key, rather than the entire Hstore object; whereas `.GetDynamicValue` is again used for referring to other columns.
`delete` operations simply delete the pair with the given key.
Execution of an operation will be skipped if the given key does not exists and the parameter `skip_not_exist` is set to true, which is also the default behavior.
Execution of an operation errors out if the given key does not exists and the parameter `error_not_exist` - which is false by default - is set to true; unless the operation is skipped already.

A limitation to be aware of: When using hstore transformer templates, you cannot set a value to the string literal "\<no value\>". This is because Go templates produce "\<no value\>" as output when the result is `nil`, creating an ambiguity. In such cases, `pgstream` will interpret it as `nil` and set the hstore value to `NULL` rather than storing the actual string "\<no value\>".

With the below config `pgstream` transforms the hstore values in the column `attributes` of the table `users` by:

- First, updating the value for key "email" to the masked version of it, using email masking function. If the key "email" is not found, it simply ignores it, since `error_not_exist` is not set to true explicitly and it is false by default.
- Then, deleting the pair where the key is "public_key". If there's no such key, errors out, because the parameter "error_not_exist" is set to true.
- Completely masking the value for key "private_key", using `go-masker`'s default masking function supported by `pgstream`'s templating.
- Finally, updating the value for key "newKey" to "newValue". Since "error_not_exist" is false by default, and there is no such key in the example below, this operation will be done by adding a new key-value pair.

Example input-output is given below the config.

**Example Configuration:**

```yaml
transformations:
  validation_mode: relaxed
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        attributes:
          name: hstore
          parameters:
            operations:
              - operation: set
                key: "email"
                value_template: '{{masking "email" .GetValue}}'
              - operation: delete
                key: "public_key"
                error_not_exist: true
              - operation: set
                key: "private_key"
                value_template: '{{masking "default" .GetValue}}'
              - operation: set
                key: "newKey"
                value: "newValue"
```

For input hstore value,

```
  public_key => 12345,
  private_key => 12345abcdefg,
  email => user@email.com
```

the hstore transformer with above config produces output:

```
  private_key => ************,
  email => use***@email.com
  newKey => newValue
```

</details>

 <details>
  <summary>literal_string</summary>

**Description:** Transforms all values into the given constant value.

| Supported PostgreSQL types             |
| -------------------------------------- |
| All types with a string representation |

| Parameter | Type   | Default | Required |
| --------- | ------ | ------- | -------- |
| literal   | string | N/A     | Yes      |

Below example makes all values in the JSON column `log_message` to become `{'error': null}`.
This transformer can be used for any Postgres type as long as the given string literal has the correct syntax for that type. e.g It can be "5-10-2021" for a date column, or "3.14159265" for a double precision one.

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: logs
      column_transformers:
        log_message:
          name: literal_string
          parameters:
            literal: "{'error': null}"
```

</details>
 <details>
  <summary>phone_number</summary>

**Description:** Generates anonymized phone numbers with customizable length.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter  | Type   | Default | Required | Values                | Dynamic |
| ---------- | ------ | ------- | -------- | --------------------- | ------- |
| prefix     | string | ""      | No       | N/A                   | Yes     |
| min_length | int    | 6       | No       | N/A                   | No      |
| max_length | int    | 10      | No       | N/A                   | No      |
| generator  | string | random  | No       | random, deterministic | No      |

If the prefix is set, this transformer will always generate phone numbers starting with the prefix.
`prefix` can also be a dynamic parameter, referring to some other column. Please see the below example config.

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        phone:
          name: phone_number
          parameters:
            min_length: 9
            max_length: 12
            generator: deterministic
          dynamic_parameters:
            gender:
              column: country_code
```

</details>

 <details>
  <summary>email</summary>

**Description:** Anonymizes email addresses while optionally excluding domain to anonymize.

| Supported PostgreSQL types                    |
| --------------------------------------------- |
| `text`, `varchar`, `char`, `bpchar`, `citext` |

| Parameter          | Type   | Default        | Required | Values |
| ------------------ | ------ | -------------- | -------- | ------ |
| replacement_domain | string | "@example.com" | No       |        |
| exclude_domain     | string | ""             | No       |        |
| salt               | string | "defaultsalt"  | No       |        |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: customers
      column_transformers:
        email:
          name: at_email
          parameters:
            exclude_domain: "example.com"
            salt: "helloworld"
```

**Input-Output Examples:**

| Input Email            | Configuration Parameters                                                               | Output Email                          |
| ---------------------- | -------------------------------------------------------------------------------------- | ------------------------------------- |
| `john.doe@company.org` | `exclude_domain: "company.org", salt: "helloworld"`                                    | `john.doe@company.org`                |
| `jane.doe@company.org` | `exclude_domain: "exclude.com", salt: "helloworld"`                                    | `T79P9zlFWzmT0yCUDMEE7S@example.com`  |
| `jane.doe@company.org` | `exclude_domain: "exclude.com", salt: "helloworld", replacement_domain: "@random.com"` | `6EIWw5lEa8nsY9JDOm5@random.com`      |
| `invalid-email`        | `exclude_domain: "exclude.com", salt: "helloworld"`                                    | `1fk5VLgTeoRQCCvqXFoToC1@example.com` |
| `invalid-email`        | `exclude_domain: "exclude.com", salt: "helloworld", replacement_domain: "@random.com"` | `6EIWw5lEa8nsY9JDOm5@random.com`      |

</details>

### Transformation rules

The rules for the transformers are defined in a dedicated yaml file, with the following format:

```yaml
transformations:
  validation_mode: <validation_mode> # Validation mode for the transformation rules. Can be one of strict, relaxed or table_level. Defaults to relaxed if not provided
  table_transformers: # List of table transformations
    - schema: <schema_name> # Name of the table schema
      table: <table_name> # Name of the table
      validation_mode: <validation_mode> # To be used when the global validation_mode is set to `table_level`. Can be one of strict or relaxed
      column_transformers: # List of column transformations
        <column_name>: # Name of the column to which the transformation will be applied
          name: <transformer_name> # Name of the transformer to be applied to the column. If no transformer needs to be applied on strict validation mode, it can be left empty or use `noop`
          parameters: # Transformer parameters as defined in the supported transformers documentation
            <transformer_parameter>: <transformer_parameter_value>
```

Below is a complete example of a transformation rules YAML file:

```yaml
transformations:
  validation_mode: table_level
  table_transformers:
    - schema: public
      table: users
      validation_mode: strict
      column_transformers:
        email:
          name: neosync_email
          parameters:
            preserve_length: true
            preserve_domain: true
        first_name:
          name: greenmask_firstname
          parameters:
            gender: Male
        username:
          name: greenmask_string
          parameters:
            generator: random
            min_length: 5
            max_length: 15
            symbols: "abcdefghijklmnopqrstuvwxyz1234567890"
    - schema: public
      table: orders
      validation_mode: relaxed
      column_transformers:
        status:
          name: greenmask_choice
          parameters:
            generator: random
            choices: ["pending", "shipped", "delivered", "cancelled"]
        order_date:
          name: greenmask_date
          parameters:
            generator: random
            min_value: "2020-01-01"
            max_value: "2025-12-31"
```

Validation mode can be set to `strict` or `relaxed` for all tables at once. Or it can be determined for each table individually, by setting the higher level `validation_mode` parameter to `table_level`. When it is set to strict, pgstream will throw an error if any of the columns in the table do not have a transformer defined. When set to relaxed, pgstream will skip any columns that do not have a transformer defined. Also in strict mode, all snapshot tables must be provided in the transformation config.
For details on how to use and configure the transformer, check the [transformer tutorial](tutorials/postgres_transformer.md).
