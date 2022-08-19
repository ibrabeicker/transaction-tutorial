create table company (
    id bigserial primary key,
    name text,
    document text not null unique
);

create table employee (
    id bigserial primary key,
    document text not null unique,
    salary numeric(15,2) not null,
    company_id bigint references company (id)
);