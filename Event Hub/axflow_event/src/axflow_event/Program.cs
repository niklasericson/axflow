using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Azure.EventHubs;
using System.Text;


namespace axflow_event
{
    public class Program
    {
        private static EventHubClient eventHubClient;
        private const string EhConnectionString = "Endpoint=sb://axflowdemo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=9D1el2YabA+NvpcB280EkFaZamNwYUTT04ArznMaxE0=";
        private const string EhEntityPath = "axfloweventhub";

        private static string[] nameArray = {   "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T",
                                                "P", "P", "P", "P", "P",
                                                "H", "H", "H",
                                                "C", "C",
                                                "Con", "Con", "Con", "Con", "Con", "Con",
                                                "A", "A", "A"};
        private static int numDevices = nameArray.Length;

        public static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            // Creates an EventHubsConnectionStringBuilder object from a the connection string, and sets the EntityPath.
            // Typically the connection string should have the Entity Path in it, but for the sake of this simple scenario
            // we are using the connection string from the namespace.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString)
            {
                EntityPath = EhEntityPath
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            Device[] DeviceArray = generateObjects();

            Console.WriteLine("Press ENTER to start");
            Console.ReadLine();
            Console.WriteLine("Sending");
            //while (true)
            //{
            await SendMessagesToEventHub(DeviceArray);

            //}
            await eventHubClient.CloseAsync();

            Console.WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }

        // Creates an Event Hub client and sends 100 messages to the event hub.
        private static async Task SendMessagesToEventHub(Device[] DeviceArray)
        {
            for (var i = 0; i < numDevices; i++)
            {
                Random r = new Random();
                float rf = (float)r.NextDouble();

                var message = "";
                string caseSwitch = DeviceArray[i].Type;

                try {
                   
                    switch (caseSwitch)
                    {
                        case "Pump":
                            Pump p = (Pump)DeviceArray[i];
                            p.Speed = 200*rf;
                            p.Temperature =  20* rf;
                            p.SuctionPressure = 10 * rf;
                            p.DischargePressure = 10 * rf;
                            p.FlowRate = p.Speed / 10;
                            p.Vibration = 100 * rf;
                            p.Time = DateTime.Now.ToString();
                            message = Newtonsoft.Json.JsonConvert.SerializeObject(p);
                            break;
                        case "Tank":
                            Tank t = (Tank)DeviceArray[i];
                            t.Temperature = 5*rf;
                            t.Time = DateTime.Now.ToString();
                            message = Newtonsoft.Json.JsonConvert.SerializeObject(t);
                            break;
                        default:
                            break;
                    }

                    //var message = $"Message";
                    Console.WriteLine($"Sending message: {message}");
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(10);
            }

            Console.WriteLine($"{numDevices} messages sent.");
        }

        private static Device[] generateObjects()
        {

            Device[] resultArray = new Device[numDevices];

            for (var i = 0; i < numDevices; i++)
            {
                string caseSwitch = nameArray[i];
                switch (caseSwitch)
                {
                    case "P":
                        Device p = new Pump();
                        p.Id = i;
                        p.Name = String.Join(p.Name, i);
                        resultArray[i] = p;
                        break;
                    case "H":
                        Heater h = new Heater();
                        h.Id = i;
                        h.Name = String.Join(h.Name, i);
                        resultArray[i] = h;
                        break;
                    case "C":
                        Cooler c = new Cooler();
                        c.Id = i;
                        c.Name = String.Join(c.Name, i);
                        resultArray[i] = c;
                        break;
                    case "Con":
                        Controller con = new Controller();
                        con.Id = i;
                        con.Name = String.Join(con.Name, i);
                        resultArray[i] = con;
                        break;
                    case "A":
                        Analyser a = new Analyser();
                        a.Id = i;
                        a.Name = String.Join(a.Name, i);
                        resultArray[i] = a;
                        break;
                    default:
                        Tank t = new Tank();
                        t.Id = i;
                        t.Name = String.Join(t.Name, i);
                        resultArray[i] = t;
                        break;
                }
            }
            return resultArray;
        }
    }


    class Device
    {
        public int Id { get; set; }
        public string Time { get; set; }
        public string Name { get; set; }
        public string Type { get; set; }
        public string Status { get; set; }

    }

    class Tank : Device
    {
        public float Temperature { get; set; }

        public Tank()
        {
            this.Name = "Tank";
            this.Type = this.Name;
            this.Status = "ok";
        }
    }

    class Pump : Device
    {
        public float Temperature { get; set; }
        public float Speed { get; set; }
        public float FlowRate { get; set; }
        public float SuctionPressure { get; set; }
        public float DischargePressure { get; set; }
        public float Vibration { get; set; }

        public Pump()
        {
            this.Name = "Pump";
            this.Type = this.Name;
            this.Status = "ok";
        }

    }

    class Heater : Device
    {
        public float Temperature { get; set; }

        public float FlowRate { get; set; }

        public Heater()
        {
            this.Name = "Heater";
            this.Type = this.Name;
            this.Status = "ok";
        }

    }

    class Cooler : Device
    {
        public float Temperature { get; set; }

        public float FlowRate { get; set; }

        public Cooler()
        {
            this.Name = "Cooler";
            this.Type = this.Name;
            this.Status = "ok";
        }
    }

    class Controller : Device
    {
        public int FlowRate { get; set; }

        public Controller()
        {
            this.Name = "Controller";
            this.Type = this.Name;
            this.Status = "ok";
        }
    }

    class Analyser : Device
    {
        public string Quality { get; set; }
        public int CulturePercentage { get; set; }
        public Analyser()
        {
            this.Name = "Analyser";
            this.Type = this.Name;
            this.Status = "ok";
        }
    }
}
