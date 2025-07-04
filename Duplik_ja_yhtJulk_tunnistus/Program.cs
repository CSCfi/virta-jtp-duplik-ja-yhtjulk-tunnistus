﻿using System;
using System.Data;
using System.Diagnostics;

namespace Duplik_ja_yhtJulk_tunnistus
{

    static class Globals
    {
        public static int min_vuosi;
        public static int tilaKoodi_vertailtava_julkaisu;
        public static int tilaKoodi_sisainen_duplikaatti;
        public static int tarkistusID_sisainen_duplikaatti;
        public static int tarkistusID_yhteisjulkaisu;
    }

    class Program
    {

        static void Main(string[] args)
        {


            // Ajastin joka mittaa kuinka kauan koodin ajo kestaa 
            //var watch = System.Diagnostics.Stopwatch.StartNew();
            //Debug.WriteLine("Lataus alkaa");

            bool debug = false;


            if (!debug && args.Length != 1)
            {
                Console.Write("Argumenttien määrä on väärä.");
                Environment.Exit(0);
            }

            // Palvelin
            string server;
            if (debug)
            {
                server = "dwitjutisql19";
            }
            else
            {
                server = args[0];
            }

            string ConnString = "Server=" + server + ";Trusted_Connection=true";



            // ---------------------------------------------------------------------------------------------------------------
            //
            // Ohjelman rakenne
            //
            // 1. Alustus
            // - Julkaisujen poisto taulusta ODS_julkaisutTMP
            // - ODS_julkaisutTMP taulun lukeminen muistiin ja nimikenttien muokkaus
            // - Nimikenttien kirjoitus tauluun ODS_julkaisutTMP
            // - SA_julkaisut taulun lukeminen muistiin ja nimikenttien muokkaus
            // - Nimikenttien kirjoitus tauluun SA_julkaisutTMP ja myös tauluun ODS_julkaisutTMP
            // - ISSN ja ISBN kenttien päivitys
            // - Indeksien päivitys
            //
            // 2. Tarkistus
            // - Ehdokkaiden etsintä
            // - Liputus onko duplikaatti vai yhteisjulkaisu
            //
            // 3. Toimenpiteet
            // - Duplikaattien tilan päivitys tauluun SA_Julkaisut
            // - YhteisjulkaisuID:n generointi uusille yhteisjulkaisuille
            // - YhteisjulkaisuID:n päivitys master-tauluun
            // - Kirjoitus tarkistuslokiin
            //
            // ---------------------------------------------------------------------------------------------------------------


            // Täällä ovat tarvittavat apufunktiot ja tietokantaoperaatiot
            Apufunktiot apufunktiot = new Apufunktiot();
            Tietokantaoperaatiot tietokantaoperaatiot = new Tietokantaoperaatiot(ConnString);

            // Globaalit muuttujat
            Globals.min_vuosi = 2015; // Tarkistus ja vertailu vain niille julkaisuille, joille julkaisuvuosi >= min_vuosi.
            Globals.tilaKoodi_vertailtava_julkaisu = 1; // ODS_Julkaisut-taulun vertailtavan julkaisun tilaKoodi ei saa olla -1 tai 0. Jos on -1, niin kyseessa on epävalidi julkaisu ja jos on 0, niin kyseessa on jo aikaisemmin sisäiseksi duplikaatiksi tunnistettu julkaisu       
            Globals.tilaKoodi_sisainen_duplikaatti = 0;
            Globals.tarkistusID_sisainen_duplikaatti = tietokantaoperaatiot.Hae_tarkistusID("sis_dupli");
            Globals.tarkistusID_yhteisjulkaisu = tietokantaoperaatiot.Hae_tarkistusID("sis_yhtj");



            ////// 1. Vaihe


            string taulu1 = "julkaisut_ods.dbo.SA_JulkaisutTMP";
            string taulu2 = "julkaisut_ods.dbo.ODS_JulkaisutTMP";


            //// ODS            

            // Poistetaan vertailutaulusta sellaiset julkaisut, jotka on poistettu aiemmin sekä sellaiset, jotka ovat nyt SA-taulussa eli joiden tiedot mahdollisesti muuttuneet
            tietokantaoperaatiot.Poista_poistetetut_julkaisut();

            // Luetaan ODS_Julkaisut taulusta sellaiset julkaisut, joita ei ole taulussa ODS_JulkaisutTMP, ja muokataan nimiä
            DataTable dt1 = tietokantaoperaatiot.Lue_tietokantataulu_datatauluun("ODS");

            foreach (DataRow row in dt1.Rows)
            {
                row["JulkaisunNimi"] = apufunktiot.Muokkaa_nimea(row["JulkaisunNimi"].ToString());
                row["KustantajanNimi"] = apufunktiot.Muokkaa_nimea(row["KustantajanNimi"].ToString());
                row["EmojulkaisunNimi"] = apufunktiot.Muokkaa_nimea(row["EmojulkaisunNimi"].ToString());
                row["Lehdennimi"] = apufunktiot.Muokkaa_nimea(row["Lehdennimi"].ToString());
                row["DOI"] = apufunktiot.Muokkaa_DOI(row["DOI"].ToString());
            }

            tietokantaoperaatiot.Kirjoita_datataulu_tietokantaan(dt1, taulu2);


            //// SA

            tietokantaoperaatiot.Tyhjenna_taulu(taulu1);

            // SA_Julkaisut-taulun tiedot datatauluun, jota muokataan ja joka lopuksi kirjoitetaan TMP-tauluun
            DataTable dt2 = tietokantaoperaatiot.Lue_tietokantataulu_datatauluun("SA");

            // Tarkistus onko käsiteltäviä rivejä. Jos kyseessä on txt-tiedostolla tehtävä julkaisujen poisto, sa-tauluun ei ole kirjoitettu mitään.
            if (dt2 == null || dt2.Rows.Count == 0) { goto loppu; }

            // Rivien läpikäynti
            foreach (DataRow row in dt2.Rows)
            {
                row["JulkaisunNimi"] = apufunktiot.Muokkaa_nimea(row["JulkaisunNimi"].ToString());
                row["KustantajanNimi"] = apufunktiot.Muokkaa_nimea(row["KustantajanNimi"].ToString());
                row["EmojulkaisunNimi"] = apufunktiot.Muokkaa_nimea(row["EmojulkaisunNimi"].ToString());
                row["Lehdennimi"] = apufunktiot.Muokkaa_nimea(row["Lehdennimi"].ToString());
                row["DOI"] = apufunktiot.Muokkaa_DOI(row["DOI"].ToString());
            }

            // Kirjoitus TMP-tauluun
            tietokantaoperaatiot.Kirjoita_datataulu_tietokantaan(dt2, taulu1);

            // Kirjoitus myös ODS_JulkaisutTMP-tauluun, jotta voidaan havaita samassa satsissa uutena tulevat duplikaatit ja yhteisjulkaisut.
            // Tosin kahden ensimmäistä kertaa toimitettavan julkaisun välistä duplikaatti-/yhteisjulkaisuyhteyttä ei voida havaita sellaisten tunnistusehtojen (ks. 2. vaihe) perusteella,
            // joissa käytetään ISSN:ää tai ISBN:ää, koska ne luetaan tauluista ODS_ISSN ja ODS_ISBN, ja uusien julkaisujen tiedot kirjoitetaan niihin myöhemmässä vaiheessa, proseduurissa SA_ODS_SIIRTO. 

            string[] estettava_taulu = taulu2.Split('.');
            tietokantaoperaatiot.Esta_indeksit(estettava_taulu[0], estettava_taulu[1], estettava_taulu[2]);
            tietokantaoperaatiot.Kirjoita_datataulu_tietokantaan(dt2, taulu2);

            // ISSN- ja ISBN-tietojen päivitys SA_JulkaisutTMP-tauluun
            tietokantaoperaatiot.Paivita_ISSN_ja_ISBN_tunnukset(taulu1);

            tietokantaoperaatiot.Uudelleenrakenna_indeksit(taulu1);
            tietokantaoperaatiot.Uudelleenrakenna_indeksit(taulu2);


            // ----------------------------------------------------------------------------------------------------


            ////// 2. vaihe


            /*  
                TUNNISTUS
             
                Tarkistetaan löytyykö ODS_JulkaisutTMP-taulusta sellaisia julkaisuja, jotka ovat yhteisjulkaisuja tai sisäisiä duplikaatteja SA_Julkaisut-taulun julkaisun kanssa. 
                Alla on lueteltu ehdot, jotka tarkistetaan.

                Ehto 1 (Tunnistussääntö 1): DOI (+ julkaisun nimi kun julkaisutyyppi A3, A4, B2, B3, D2, D3, E1)
                Ehto 2 (Tunnistussääntö 2 v1): ISSN1 + volyymi + numero + sivut + julkaisun nimi
                Ehto 3 (Tunnistussääntö 2 v2): ISSN2 + volyymi + numero + sivut + julkaisun nimi

                Poistettu 11/2016 (ehtoihin 2 ja 3 tehtiin muutos, että niissä on mukana myös julkaisun nimi)
                (Ehto 4: ISSN1 + volyymi + numero + julkaisun nimi)
                (Ehto 5: ISSN2 + volyymi + numero + julkaisun nimi)

                Ehto 6 (Tunnistussääntö 3): julkaisutyyppi + kustantaja + julkaisun nimi (koskee julkaisutyyppeja C1, D5, E1, E2 pl.Introduction, Esipuhe, Johdanto)
                Ehto 7 (Tunnistussääntö 4): emojulkaisun nimi + julkaisun nimi (koskee julkaisutyyppeja A3, A4, B2, B3, D1, D2, D3, E1 pl.Introduction, Esipuhe, Johdanto)
                Ehto 8 (Tunnistussääntö 5 v1): ISBN1 + julkaisun nimi
                Ehto 9 (Tunnistussääntö 5 v2): ISBN2 + julkaisun nimi

                Lisätty 2/2022
                Ehto 10 (Tunnistussääntö 6): Julkaisutyyppi + julkaisun nimi + lehden nimi + julkaisuvuosi (koskee julkaisutyyppeja D1)
                Ehto 11 (Tunnistussääntö 7): Julkaisutyyppi + julkaisun nimi + kustantajan nimi + julkaisuvuosi (koskee julkaisutyyppeja D4)

                Lisätty 11/2023
                Ehto 12 (Tunnistussääntö 8): Julkaisutyyppi + julkaisun nimi + julkaisuvuosi + AVsovellustyyppiKoodi (koskee julkaisutyyppejä I1, I2)
                Ehto 13 (Tunnistussääntö 9): Julkaisutyyppi (ainoastaan Virran sisäisessä käytössä) + julkaisun nimi + julkaisuvuosi (koskee julkaisuita joiden muotokoodi on posteri tai abstrakti)
                
                RAJOITTEET
                Tunnistussäännöt 1 - 5
                    - julkaisut_ods.dbo.EiYhteisjulkaisuJulkaisutyyppiparit
                Tunnistussäännöt 1 - 7
                    - julkaisut_ods.dbo.EiDuplikaattiTarkistusta
                Tunnistussäännöt 2 - 5
                    - DOIt eivät saa olla erisuuria
                
                
                LIPUTUS
             
                Sisäinen duplikaatti: 
                1. Duplikaatin Yhteisjulkaisu_ID = 0, sama organisaatio, eri julkaisunOrgTunnus
                2. Duplikaatin Yhteisjulkaisu_ID <> 0, organisaatio on mukana yhteisjulkaisussa, julkaisunOrgTunnus on eri

                Yhteisjulkaisu:
                1. Duplikaatin Yhteisjulkaisu_ID = 0, eri organisaatio
                2. Duplikaatin Yhteisjulkaisu_ID <> 0, organisaatio ei mukana yhteisjulkaisussa

            */


            //tietokantaoperaatiot.uudelleenjarjesta_indeksit("julkaisut_mds.koodi.julkaisuntunnus");


            /* 
                Silmukka, jossa julkaisuille etsitään potentiaalinen duplikaatti/yhteisjulkaisu ja katsotaan täyttääkö ehdokas yllä olevat ehdot.
                Jos ehdokas ei täytä ehtoja niin julkaisu on mukana seuraavalla kierroksella.
            */

            int ehdot_lkm = 13;
            for (int i = 1; i <= ehdot_lkm; i++) {
                if (i == 4 || i == 5) { continue; }
                tietokantaoperaatiot.Etsi_yhteisjulkaisut(i);
                tietokantaoperaatiot.Liputa_duplikaatit(i);
                tietokantaoperaatiot.Liputa_yhteisjulkaisut(i);
            }

            // Muutosten jälkeen taas indeksien päivitys
            tietokantaoperaatiot.Uudelleenjarjesta_indeksit(taulu1);


            // ----------------------------------------------------------------------------------------------------


            ////// 3. vaihe


            /*
                Sisäinen duplikaatti: 
                -> Julkaisun tilan päivitys tauluun julkaisut_ods.dbo.SA_Julkaisut ja kirjoitus tarkistuslokiin

                Yhteisjulkaisu:
                - Jos yhteisjulkaisua ei ole vielä olemassa
                -> Yhteisjulkaisu ID:n generointi ja päivitys molemmille julkaisuille tauluun julkaisut_mds.koodi.julkaisuntunnus sekä kirjoitus tarkistuslokiin
                - Jos yhteisjulkaisu on olemassa
                -> Yhteisjulkaisu ID:n päivitys tauluun julkaisut_mds.koodi.julkaisuntunnus ja kirjoitus tarkistuslokiin
            */


            // Jos duplikaatti
            tietokantaoperaatiot.Paivita_duplikaatit();

            // Jos yhteisjulkaisu
            tietokantaoperaatiot.Luo_yhteisjulkaisut();
            tietokantaoperaatiot.Paivita_yhteisjulkaisut();

            // Tarkistuslokiin
            tietokantaoperaatiot.Kirjoita_tarkistuslokiin();


        // ----------------------------------------------------------------------------------------------------

            loppu:
            ;
            //Console.Write("Loppu");
            //Console.ReadLine();

            // Ajastin, loppu
            //watch.Stop();
            //var elapsedMs = watch.ElapsedMilliseconds;
            //Console.Write("Koko ajon kesto: " + elapsedMs);
        }

    }

}