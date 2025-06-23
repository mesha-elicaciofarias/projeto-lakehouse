# Objetivo
O objetivo desta POC (Prova de conceito) e montar uma plataforma de dados mínima usando ferramentas de código aberta. 

# Objetivos específicos
- Montar uma plataforma de dados mínima com as camadas de orquestração, processamento e armazenamento
- Usar ferramentas de código aberto

# Ferramentas usadas
- [airflow - 3.0.2](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [pyspark - 4.0.0](https://spark.apache.org/docs/latest/api/python/index.html#)
- [delta lake - 4.0.0](https://docs.delta.io/latest/index.html)
- [minio - RELEASE.2025-04-22T22-12-26Z](https://min.io/docs/minio/kubernetes/upstream/index.html)

# Configuração dos serviços
> Atenção! Este projeto deve ser executado em uma maquina Linux ou wsl2 do windows
> 
> Para saber mais sobre wsl2 click [aqui](https://learn.microsoft.com/pt-br/windows/wsl/about)

## 0 - Abra o terminal de sua preferência
- No Windows, click [aqui](https://elsefix.com/pt/tech/tejana/how-to-use-windows-terminal-in-windows-10-beginners-guide) para saber como abrir um terminal.
- No Linux, click [aqui](https://pt.linux-console.net/?p=12268) para saber como abrir um terminal.

## 1 - Clone do repositorio
- Click [aqui](https://docs.github.com/pt/repositories/creating-and-managing-repositories/cloning-a-repository) e siga o passo de como clonar um respositório do github

## 2 - Mude para a pasta do projeto
~~~bash
$ cd projeto-lakehouse
~~~

## 4 - Configure uma rede externa do docker
~~~bash
$ docker network create projeto-lakehouse
~~~

## 5 - Crie o arquivo .env baseado no .env.template
~~~bash
$ cp .env.template .env
~~~

## 6 - Abra um editor o editor de código de sua preferência.
> Sugestão! Use o vscode.
- No Windows com wsl2, click [aqui](https://learn.microsoft.com/pt-br/windows/wsl/tutorials/wsl-vscode) para saber como instalar e usar.
- No Linux, click [aqui](https://code.visualstudio.com/docs/setup/linux) para saber como instalar e usar.

## 7 - Abra o arquivo .env e preencha os valores das variáveis

## 8 - Volte para o terminal e execute o comando abaixo para iniciar os serviços
~~~bash
$ docker compose up
~~~
Este comando deixa o terminal preso, se preferir pode usar o comando abaixo para iniciar os serviços e liberar o terminal
~~~bash
$ docker compose up -d
~~~
