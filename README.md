# 

## Objetivo

Esse projeto tem por objetivo exemplificar e explicar as maneiras certas e erradas de se utilizar alguns mecanismos 
relacionados ao banco de dados dentro de um ambiente Java e Spring, tais como transações, lock, EntityManager 
e leitura, remoção e persistência de altos volumes de dados.

## Do que é feito

Uma aplicação Spring Boot que cria um container Docker de um banco PostgreSQL para execução de testes. As classes no 
módulo `main` são apenas para exemplificar uma sistema que lida com duas entidades relacionadas (Empresa e 
Funcionário). O módulo `test` contém a parte principal do projeto, onde estão seis classes de testes, cada uma abordando
um assunto e contendo métodos de teste comentados sobre o comportamento observado em cada caso.

## Como usar

Execute os testes do projeto, pela IDE ou pelo comando `./gradlew test`. Alguns testes falharão intencionalmente para 
evidenciar um problema que seria encontrado caso o desenvolvedor usasse aquela abordagem.

As mensagens de log impressas na execução dos testes também auxiliam na compreensão do comportamento dos frameworks utilizados. 



