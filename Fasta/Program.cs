using System;
using System.Collections.Generic;
using System.IO;
using MySql.Data.MySqlClient;

namespace Fasta
{
    class Program
    {
        static void Main()
        {
            var baltoPath = "D:/Master/Norelation databases/Fasta/Fasta/data/Balto_Slavic.fasta";
            var ukrPath = "D:/Master/Norelation databases/Fasta/Fasta/data/Ukraine.fasta";

            var baltoFasta = Fasta(baltoPath);
            var baltoGP = GpBalto();

            var ukrFasta = Fasta(ukrPath);
            var urkGP = Gp();

            Insert(urkGP, ukrFasta, "Ukraine");
            Insert(baltoGP, baltoFasta, "BaltoSlavic");

            string cs = @"server=localhost;userid=root;password=qwerty;database=laboratory_fasta";
            using var con = new MySqlConnection(cs);
            con.Open();
            using var cmd = new MySqlCommand();
            cmd.Connection = con;
            cmd.CommandText = "DELETE FROM records WHERE records.id = 'KT262553.1' or  records.id = 'KT262558.1'";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "DELETE FROM fasta WHERE id = 'KT262553.1' or  id = 'KT262558.1'";
            cmd.ExecuteNonQuery();
            con.Close();

            // InsertUkraine();
            // var res = GpBalto();
            //var set = new SortedSet<string>();
            //var not = 0;
            //foreach (var dic in res)
            //{
            //    if (dic.ContainsKey("DISTRICT"))
            //        set.Add(dic["DISTRICT"]);
            //    else
            //    {
            //        not++;
            //        foreach (var k in dic.Keys)
            //        {
            //            Console.WriteLine(k+" "+dic[k]);
            //        }

            //    }
            //}

            //foreach (var dist in set)
            //{
            //    Console.WriteLine(dist);
            //}

            //  Console.WriteLine(not);
            //var balto = Fasta();
            //foreach (var ll in balto)
            //{
            //    foreach (var k in ll)
            //    {
            //       Console.WriteLine(k.Key+" "+k.Value);  
            //    }  
            //}

        }


        private static void Insert(List<Dictionary<string, string>> records, List<List<KeyValuePair<string, string>>> recordsFasta, string database)
        {
            string cs = @"server=localhost;userid=root;password=qwerty;database=laboratory_fasta";
            using var con = new MySqlConnection(cs);
            con.Open();
            using var cmd = new MySqlCommand();
            cmd.Connection = con;
            string statement;
            //var recordsFasta = Fasta();
            foreach (var rRecord in recordsFasta)
            {
                cmd.CommandText = $"INSERT INTO fasta(id, fasta) VALUES('{rRecord[0].Value}','{rRecord[2].Value}')";
                cmd.ExecuteNonQuery();
            }
            //  var records = Gp();
            var regions = new Dictionary<string, int>();
            var country = new Dictionary<string, int>();
            var haplo = new SortedSet<string>();

            foreach (var dictionary in records)
            {
                if (!haplo.Contains(dictionary["HAPLOGROUP"]))
                {
                    // check database
                    statement = $"SELECT id FROM haplogroup WHERE id = '{dictionary["HAPLOGROUP"]}'";
                    cmd.CommandText = statement;
                    cmd.Connection = con;
                    //var str = cmd.ExecuteScalar().ToString();
                    var obj = cmd.ExecuteScalar();
                    var str = obj == null ? "" : obj.ToString();
                    // already saved
                    if (string.IsNullOrEmpty(str))
                    {
                        statement = dictionary.ContainsKey("HAPLOGROUP COMMENT")
                            ? $"INSERT INTO haplogroup(id, comment) VALUES('{dictionary["HAPLOGROUP"]}','{dictionary["HAPLOGROUP COMMENT"]}')"
                            : $"INSERT INTO haplogroup(id) VALUES('{dictionary["HAPLOGROUP"]}')";

                        cmd.CommandText = statement;
                        cmd.ExecuteNonQuery();
                        haplo.Add(dictionary["HAPLOGROUP"]);
                    }
                    else
                        haplo.Add(dictionary["HAPLOGROUP"]);
                }

                int countryId = 0;
                if (!country.ContainsKey(dictionary["COUNTRY"]))
                {
                    // check database
                    statement = $"SELECT id FROM country WHERE country_name = '{dictionary["COUNTRY"]}'";
                    cmd.CommandText = statement;
                    var obj = cmd.ExecuteScalar();
                    var str = obj == null ? "" : obj.ToString();
                    // already saved

                    if (str == null || str.Length <= 0)
                    {
                        cmd.CommandText =
                            $"INSERT INTO country(country_name, country_symbol) VALUES('{dictionary["COUNTRY"]}','{dictionary["COUNTRY"].ToUpper().Substring(0, 3)}')";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = statement;
                        str = cmd.ExecuteScalar().ToString();
                        if (str != null)
                        {
                            countryId = int.Parse(str);
                            country.Add(dictionary["COUNTRY"], countryId);
                        }

                    }

                    
                    else
                    {
                        countryId = int.Parse(str);
                        country.Add(dictionary["COUNTRY"], countryId);
                    }
                }
                else
                    countryId = country[dictionary["COUNTRY"]];

                if (!regions.ContainsKey(dictionary["REGION"]))
                {
                    //check database
                    statement = $"SELECT idregions FROM regions WHERE idregions = '{dictionary["REGION"]}'";
                    cmd.CommandText = statement;
                    var obj = cmd.ExecuteScalar();
                    var res = obj == null ? "" : obj.ToString();
                    //var res = cmd.ExecuteScalar().ToString();
                    if (string.IsNullOrEmpty(res))
                    {
                        if (dictionary.ContainsKey("DISTRICT"))
                        {
                            cmd.CommandText =
                                $"INSERT INTO regions(idregions, region_name, country) VALUES(\"{dictionary["REGION"]}\",\"{dictionary["DISTRICT"]}\", {countryId});";
                            cmd.ExecuteNonQuery();
                        }
                        else
                        {
                            cmd.CommandText =
                                $"INSERT INTO regions(idregions, country) VALUES('{dictionary["REGION"]}', {countryId})";
                            cmd.ExecuteNonQuery();
                        }
                    }
                    regions.Add(dictionary["REGION"], 0);
                }

                cmd.CommandText = $"INSERT INTO records (`id`,`isolate`, `counter`, `database`, `isolate_code`, `haplogroup`) " +
                                  $"VALUES ('{dictionary["VERSION"]}','{dictionary["ISOLATE"]}', 1, '{database}', '{dictionary["REGION"]}', '{dictionary["HAPLOGROUP"]}');";
                cmd.ExecuteNonQuery();
            }

            foreach (var c in country)
            {
                Console.WriteLine(c.Value + " " + c.Key);
            }

            con.Close();
        }


        static List<Dictionary<string, string>> GpBalto()
        {
            var reader = File.ReadAllText("D:/Master/Norelation databases/Fasta/Fasta/data/Balto_Slavic.gp");
            reader = reader.Replace("  ", " ").Replace("  ", " ")
                           .Replace("  ", " ").Replace("  ", " ")
                           .Replace("  ", " ");

            var split = reader.Split("//");
            List<string[]> parcedRecord = new List<string[]>();
            foreach (var record in split)
            {
                parcedRecord.Add(record.Split('\n'));
            }

            var commentHaplogroup = false;

            List<Dictionary<string, string>> keyParcedRecord = new List<Dictionary<string, string>>();
            foreach (var record in parcedRecord)
            {
                var value = "";
                string key;
                Dictionary<string, string> recordKeys = new Dictionary<string, string>();
                foreach (var row in record)
                {

                    var temp = row.Split(" ");

                    if (temp[0] == "VERSION")
                    {
                        value = temp[1];
                        key = temp[0];
                    }
                    else if (temp.Length <= 1) continue;
                    else if (temp[1].StartsWith("/isolate"))
                    {
                        var res = temp[1].Split("=");
                        value = res[1].Substring(1, res[1].Length - 2);
                        string region = "";
                        foreach (var ch in value)
                        {
                            if (char.IsDigit(ch)) break;
                            region += ch;
                        }
                        recordKeys.Add("REGION", region);
                        key = "ISOLATE";
                    }
                    else if (temp[1].StartsWith("/isolation_source"))
                    {
                        var res = row.Split("=");
                        value = res[1].Substring(1, res[1].Length - 2);
                        key = "DISTRICT";
                        recordKeys.Add(key, value);
                        key = "ISOLATION SOURCE";
                    }
                    else if (temp[1].StartsWith("/haplogroup"))
                    {
                        var res = temp[1].Split("=");
                        value = res[1].Substring(1);
                        if (value.Contains("\"")) value = value.Substring(0, value.Length - 1);
                        key = "HAPLOGROUP";
                        var ro = String.Join(" ", temp[2..]);
                        bool haploComment = ro.Contains('(');
                        if (haploComment)
                        {
                            recordKeys.Add(key, value);
                            commentHaplogroup = true;
                            value = ro.Substring(0, ro.Length - 1);
                            continue;
                        }

                    }
                    else if (temp[1].StartsWith("/country"))
                    {
                        if (commentHaplogroup)
                        {
                            key = "HAPLOGROUP COMMENT";
                            recordKeys.Add(key, value);
                            commentHaplogroup = false;
                        }
                        var res = row.Split("=");
                        var countryRegion = res[1].Substring(1, res[1].Length - 2).Split(':');
                        value = countryRegion[0];
                        key = "COUNTRY";
                        if (countryRegion.Length > 1)
                        {
                            recordKeys.Add(key, value);
                            value = countryRegion[1].TrimStart();
                            key = "DISTRICT";
                        }


                    }
                    else if (commentHaplogroup)
                    {
                        var res = row.TrimStart().TrimEnd();
                        value = value + " " + res.Substring(0, res.Length - 1);
                        key = "HAPLOGROUP COMMENT";
                        commentHaplogroup = false;
                    }
                    else
                        continue;
                    recordKeys.Add(key, value);
                }
                if (recordKeys.Count > 1)
                    keyParcedRecord.Add(recordKeys);
            }

            return keyParcedRecord;
        }

        static List<Dictionary<string, string>> Gp()
        {
            var reader = File.ReadAllText("D:/Master/Norelation databases/Fasta/Fasta/data/Ukraine.gp");
            reader = reader.Replace("  ", " ").Replace("  ", " ")
                           .Replace("  ", " ").Replace("  ", " ")
                           .Replace("  ", " ");

            var split = reader.Split("//");
            List<string[]> parcedRecord = new List<string[]>();
            foreach (var record in split)
            {
                parcedRecord.Add(record.Split('\n'));
            }

            var commentHaplogroup = false;

            List<Dictionary<string, string>> keyParcedRecord = new List<Dictionary<string, string>>();
            foreach (var record in parcedRecord)
            {
                var value = "";
                string key;
                Dictionary<string, string> recordKeys = new Dictionary<string, string>();
                foreach (var row in record)
                {
                    
                    var temp = row.Split(" ");

                    if (temp[0] == "VERSION")
                    {
                        value = temp[1];
                        key = temp[0];
                    }
                    else if (temp.Length <= 1) continue;
                    else if (temp[1].StartsWith("/isolate"))
                    {
                        var res = temp[1].Split("=");
                        value = res[1].Substring(1, res[1].Length - 2);
                        var region = value.Split("-")[0];
                        recordKeys.Add("REGION", region);
                        key = "ISOLATE";
                    }
                    else if (temp[1].StartsWith("/isolation_source"))
                    {
                        var res = row.Split("=");
                        value = res[1].Substring(1, res[1].Length - 2);
                        key = "ISOLATION SOURCE";
                    }
                    else if (temp[1].StartsWith("/haplogroup"))
                    {
                        var res = temp[1].Split("=");
                        value = res[1].Substring(1);
                        if (value.Contains("\"")) value = value.Substring(0, value.Length - 1);
                        key = "HAPLOGROUP";
                        var ro = String.Join(" ", temp[2..]);
                        bool haploComment = ro.Contains('(');
                        if (haploComment)
                        {
                            recordKeys.Add(key, value);
                            commentHaplogroup = true;
                            value = ro.Substring(0, ro.Length - 1);
                            continue;
                        }

                    }
                    else if (temp[1].StartsWith("/country"))
                    {
                        if (commentHaplogroup)
                        {
                            key = "HAPLOGROUP COMMENT";
                            recordKeys.Add(key, value);
                            commentHaplogroup = false;
                        }
                        var res = row.Split("=");
                        var countryRegion = res[1].Substring(1, res[1].Length - 2).Split(':');
                        value = countryRegion[0];
                        key = "COUNTRY";
                        if (countryRegion.Length > 1)
                        {
                            recordKeys.Add(key, value);
                            value = countryRegion[1].TrimStart();
                            key = "DISTRICT";
                        }
                       

                    }
                    else if (commentHaplogroup)
                    {
                        var res = row.TrimStart().TrimEnd();
                        value = value + " " + res.Substring(0, res.Length - 1); 
                        key = "HAPLOGROUP COMMENT";
                        commentHaplogroup = false;
                    }
                    else
                        continue;
                    recordKeys.Add(key, value);
                }
                if(recordKeys.Count>1)
                    keyParcedRecord.Add(recordKeys);
            }

            return keyParcedRecord;
        }
        
        static List<List<KeyValuePair<string, string>>> Fasta(string path)
        {
            var fasta = File.ReadAllText(path);
            var records = fasta.Split(">");

            List<List<KeyValuePair<string, string>>> parcedRecords = new List<List<KeyValuePair<string, string>>>();
            for (int i = 1; i < records.Length; i++)
            {
                List<KeyValuePair<string, string>> temp = new List<KeyValuePair<string, string>>();
                var parced = (records[i].Split(" "));
                temp.Add(new KeyValuePair<string, string>("VERSION", parced[0]));
                temp.Add(new KeyValuePair<string, string>("ISOLATE", parced[4]));
                var fastaArray = parced[8].Split('\n');
                fastaArray = fastaArray[1..];
                temp.Add(new KeyValuePair<string, string>("FASTA", String.Join("", fastaArray)));
                parcedRecords.Add(temp);
            }

            return parcedRecords;
        }


    }
}
