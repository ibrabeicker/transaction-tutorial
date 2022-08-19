create table company (
    id bigserial primary key,
    document text not null
);

create table employee (
    id bigserial primary key,
    document text not null,
    salary numeric(15,2) not null,
    company_id bigint references company (id)
);