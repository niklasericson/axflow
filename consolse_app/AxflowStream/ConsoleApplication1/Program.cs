using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Threading;

namespace axflow_event
{
    public class Program
    {
        private static EventHubClient eventHubClient;
        private const string EhConnectionString = "Endpoint=sb://axflowdemo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=9D1el2YabA+NvpcB280EkFaZamNwYUTT04ArznMaxE0=";
        private const string EhEntityPath = "axfloweventhub";

        private static string[] itemArray = {   "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T", "T",
                                                "P", "P", "P", "P", "P",
                                                "H", "H", "H",
                                                "C", "C",
                                                "Con", "Con", "Con", "Con", "Con", "Con",
                                                "A", "A", "A"};

        private static string[] itemNameArray = {   "Collection Tank", "Separator", "Separator tank", "Milk powder tank", "Homogeniser",
                                                    "Culture tank","Incubation tank 1","Incubation tank 2","Buffer tank","Fruit tank",
                                                    "Flavouring tank","Syrup tank","Culture tank 1","Culture tank 2","Mixer","Filling area","Incubation chamber","Separator pump","Pre-heater pump","Buffer pump",
                                                    "Mixer pump 1","Mixer pump 2","Pre-heater 1","Pre-heater 2","Heater","Incubation cooler","Buffer cooler",
                                                    "Metering and dosing device 1","Metering and dosing device 2","Metering and dosing device 3","Metering and dosing device 4","Metering and dosing device 5",
                                                    "Filling machine","Raw material analyser","Finish product analyser 1","Finish product analyser 2"};

        private static int numDevices = itemArray.Length;

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

            ConsoleKeyInfo cki = new ConsoleKeyInfo();

            do
            {
                Console.WriteLine("\nPress: \n 'f' to inject fault,\n 'r' to reset \n 'x' key to quit.");

                while (Console.KeyAvailable == false)

                await SendMessagesToEventHub(DeviceArray);

                cki = Console.ReadKey(true);
                if (cki.Key == ConsoleKey.F)
                {
                    Console.WriteLine("You pressed the '{0}' key.", cki.Key);
                    DeviceArray[20].Status = "Replace";
                    DeviceArray[20].Speed = 5;
                    Console.WriteLine("A fault was injected");
                }
                if (cki.Key == ConsoleKey.R)
                {
                    Console.WriteLine("You pressed the '{0}' key.", cki.Key);
                    DeviceArray[20].Status = "ok";
                    DeviceArray[20].Speed = DeviceArray[21].Speed;
                    Console.WriteLine("The fault was reset");
                }

            } while (cki.Key != ConsoleKey.X);

            await eventHubClient.CloseAsync();
            Console.WriteLine("Good bye!");
            Thread.Sleep(250);
        }

        // Creates an Event Hub client and sends the device info to the event hub.
        private static async Task SendMessagesToEventHub(Device[] DeviceArray)
        {
            DateTime date = DateTime.Now.ToLocalTime();
            for (var i = 0; i < numDevices; i++)
            {
                Random r = new Random();
                float rf = (float)r.NextDouble();

                var message = "";
                string caseSwitch = DeviceArray[i].Type;

                try
                {

                    switch (caseSwitch)
                    {
                        case "Pump":
                            Pump p = (Pump)DeviceArray[i];
                            p.Speed = 100 + rf; //rpm
                            p.Temperature = 70 + rf; // degrees Celcius
                            p.SuctionPressure = 2 + (float)0.1 * rf; // Bar
                            p.DischargePressure = 5 + (float)0.1 * rf; // Bar
                            p.FlowRate = p.Speed / 10; // dm3/s
                            p.Vibration = 100 * rf;
                            p.Time = date;
                            message = Newtonsoft.Json.JsonConvert.SerializeObject(p);
                            break;
                        case "Tank":
                            Tank t = (Tank)DeviceArray[i];
                            t.Temperature = 5 + (float)0.1 * rf; // degrees Celcius
                            t.Time = date;
                            message = Newtonsoft.Json.JsonConvert.SerializeObject(t);
                            break;
                        case "Cooler":
                            Cooler c = (Cooler)DeviceArray[i];
                            c.Temperature = -10 + (float)0.1 * rf; // degrees Celcius
                            c.Time = date;
                            message = Newtonsoft.Json.JsonConvert.SerializeObject(c);
                            break;
                        case "Heater":
                            Heater h = (Heater)DeviceArray[i];
                            h.Temperature = 200 + 10 * rf; // degrees Celcius
                            h.Time = date;
                            message = Newtonsoft.Json.JsonConvert.SerializeObject(h);
                            break;
                        case "Controller":
                            Controller con = (Controller)DeviceArray[i];
                            con.Temperature = 20 + (float)0.1 * rf; // degrees Celcius
                            con.FlowRate = (float)(0.1 + rf);
                            con.Time = date;
                            message = Newtonsoft.Json.JsonConvert.SerializeObject(con);
                            break;
                        case "Analyser":
                            Analyser a = (Analyser)DeviceArray[i];
                            float percentage = 10 * rf; //%
                            a.Temperature = 20 + (float)0.1 * rf; // degrees Celcius
                            a.CulturePercentage = (int)percentage;
                            a.Time = date;
                            message = Newtonsoft.Json.JsonConvert.SerializeObject(a);
                            break;
                        default:
                            break;
                    }

                    //var message = $"Message";
                    //Console.WriteLine($"Sending message: {message}");
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
                string caseSwitch = itemArray[i];
                string itemName = itemNameArray[i];

                switch (caseSwitch)
                {
                    case "P":
                        Device p = new Pump();
                        p.Id = i;
                        p.Name = itemName;
                        resultArray[i] = p;
                        break;
                    case "H":
                        Heater h = new Heater();
                        h.Id = i;
                        h.Name = itemName;
                        resultArray[i] = h;
                        break;
                    case "C":
                        Cooler c = new Cooler();
                        c.Id = i;
                        c.Name = itemName;
                        resultArray[i] = c;
                        break;
                    case "Con":
                        Controller con = new Controller();
                        con.Id = i;
                        con.Name = itemName;
                        resultArray[i] = con;
                        break;
                    case "A":
                        Analyser a = new Analyser();
                        a.Id = i;
                        a.Name = itemName;
                        resultArray[i] = a;
                        break;
                    default:
                        Tank t = new Tank();
                        t.Id = i;
                        t.Name = itemName;
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
        public DateTime Time { get; set; }
        public string Name { get; set; }
        public string Type { get; set; }
        public string Status { get; set; }
        public float Temperature { get; set; }
        public float Speed { get; set; }
        public float FlowRate { get; set; }
        public float SuctionPressure { get; set; }
        public float DischargePressure { get; set; }
        public float Vibration { get; set; }
        public int CulturePercentage { get; set; }

    }

    class Tank : Device
    {

        public Tank()
        {
            this.Name = "Tank";
            this.Type = this.Name;
            this.Status = "ok";
        }
    }

    class Pump : Device
    {
        public Pump()
        {
            this.Name = "Pump";
            this.Type = this.Name;
            this.Status = "ok";
        }

    }

    class Heater : Device
    {

        public Heater()
        {
            this.Name = "Heater";
            this.Type = this.Name;
            this.Status = "ok";
        }

    }

    class Cooler : Device
    {
        public Cooler()
        {
            this.Name = "Cooler";
            this.Type = this.Name;
            this.Status = "ok";
        }
    }

    class Controller : Device
    {

        public Controller()
        {
            this.Name = "Controller";
            this.Type = this.Name;
            this.Status = "ok";
        }
    }

    class Analyser : Device
    {
        public Analyser()
        {
            this.Name = "Analyser";
            this.Type = this.Name;
            this.Status = "ok";
        }
    }
}
