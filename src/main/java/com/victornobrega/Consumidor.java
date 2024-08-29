package com.victornobrega;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Consumidor {
    public static void main(String[] args) throws Exception {
        System.out.println("Consumidor no ar");

        String Task_Queue = "task_queue";

        //criando a fábrica de conexões e criando uma conexão
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("mqadmin");
        connectionFactory.setPassword("Admin123XX_");
        connectionFactory.setHost("localhost");
        Connection conexao = connectionFactory.newConnection();

        //criando um canal e declarando a fila
        Channel channel = conexao.createChannel();
        channel.basicQos(1);
        boolean duravel = true;
        channel.queueDeclare(Task_Queue, duravel, false, false, null);

        //Definindo a funcao callback, que será executada quando receber um objeto, no caso a mensagem.
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String mensagem = new String (delivery.getBody(), "UTF-8");

            System.out.println ("[x] Recebido '" + mensagem + "'");
            try {
                doWork (mensagem);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                System.out.println ("[x] Feito");
                channel.basicAck(delivery.getEnvelope(). getDeliveryTag(), false);
            }
        };

        //Consumindo da fila
        boolean autoAck = false; // ack é feito aqui. Como está autoAck, enviará automaticamente
        channel.basicConsume (Task_Queue, autoAck, deliverCallback, consumerTag -> {});
        System.out.println("Continuarei executando outras atividades enquanto não chega mensagem...");
    }

    // tarefa falsa para simular o tempo de execução
    private static void doWork (String task) throws InterruptedException {
        for (char ch: task.toCharArray ()) {
            if (ch == '.') {
                try {
                    Thread.sleep (1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
