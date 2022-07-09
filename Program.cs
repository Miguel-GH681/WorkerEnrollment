
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;
using ServiceReference;
using WorkerEnrollmentProcess.Models;

public class Program{
    private static AppLog appLog = new AppLog();
    private static IModel channel = null;

    public static void Main(string[] args){
        Log.Logger = new LoggerConfiguration()
        .WriteTo
        .File("WorkerEnrollmentManagement.out", Serilog.Events.LogEventLevel.Debug, "{Message:lj}{NewLine}", encoding: Encoding.UTF8)
        .CreateLogger();

        appLog.ResponseTime = Convert.ToInt16(DateTime.Now.ToString("fff"));
        appLog.DateTime = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss");
        appLog.ResponseCode = 0;
        
        IConnection conexion = null;
        var connectionFactory = new ConnectionFactory();
        connectionFactory.Port = 5672;
        connectionFactory.UserName = "guest";
        connectionFactory.Password = "guest";
        conexion = connectionFactory.CreateConnection();
        channel = conexion.CreateModel();

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += ConsumerReceived;
        var consumerTag = channel.BasicConsume("kalum.queue.rest", true, consumer);
        Console.WriteLine("Presione enter para finalizar el proceso");
        Console.ReadLine();
    }

    private static async void ConsumerReceived(object? sender, BasicDeliverEventArgs e){
        string message = Encoding.UTF8.GetString(e.Body.ToArray());
        EnrollmentRequest request = JsonSerializer.Deserialize<EnrollmentRequest>(message);
        ImprimirLog(0, JsonSerializer.Serialize(request), "Debug");
        EnrollmentResponse enrollmentResponse = await ClientWebService(request);
        if(enrollmentResponse != null && enrollmentResponse.Codigo != 201){
            channel.BasicPublish("", "kalum.queue.failed.enrollment", null, Encoding.UTF8.GetBytes(message));
        } else{
            appLog.Message = "Se procesó el proceso de inscripción de forma exitosa";
            ImprimirLog(201, JsonSerializer.Serialize(appLog), "Information");
        }
    }

    public static async Task<EnrollmentResponse> ClientWebService(EnrollmentRequest request){
        EnrollmentResponse enrollmentResponse = null;
        try{
            var client = new EnrollmentServiceClient(EnrollmentServiceClient.EndpointConfiguration.BasicHttpBinding_IEnrollmentService_soap, "http://localhost:5239/EnrollmentService.asmx");
            var response = await client.EnrollmentProcessAsync(request);
            enrollmentResponse = new EnrollmentResponse{ Codigo = response.Body.EnrollmentProcessResult.Codigo,
                                                         Respuesta = response.Body.EnrollmentProcessResult.Respuesta, 
                                                         Carne = response.Body.EnrollmentProcessResult.Carne};
            if(enrollmentResponse.Codigo == 503){
                ImprimirLog(503, enrollmentResponse.Respuesta, "Error");
            } else{
                ImprimirLog(201, $"Se finalizó el proceso de inscripción con el carné {enrollmentResponse.Carne}", "Information");
            }
        } catch(Exception e){
            enrollmentResponse = new EnrollmentResponse {Codigo = 500, Respuesta = e.Message, Carne = "0"};
            ImprimirLog(500, $"Error en el sistema: {e.Message}", "Error");
        }
        return enrollmentResponse;
    }

    private static void ImprimirLog(int responseCode, string message, string typeLog){
        appLog.ResponseCode = responseCode;
        appLog.Message = message;
        appLog.ResponseTime = Convert.ToInt16(DateTime.Now.ToString("fff")) - appLog.ResponseTime;
        if(typeLog.Equals("Error")){
            appLog.Level = 40;
            Log.Error(JsonSerializer.Serialize(appLog));
        } else if(typeLog.Equals("Information")){
            appLog.Level = 20;
            Log.Information(JsonSerializer.Serialize(appLog));
        } else{
            appLog.Level = 10;
            Log.Debug(JsonSerializer.Serialize(appLog));
        }
    }
}