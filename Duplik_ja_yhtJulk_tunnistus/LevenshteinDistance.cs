using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Duplik_ja_yhtJulk_tunnistus
{
    class LevenshteinDistance
    {

        ///////////////////////////////////////////
        //Tassa on Levenshtein-distance-algoritmi//
        ///////////////////////////////////////////
        //Tassa on Levenshtein-distance-algoritmi//
        ///////////////////////////////////////////
        //Tassa on Levenshtein-distance-algoritmi//
        ///////////////////////////////////////////
        //Tassa on Levenshtein-distance-algoritmi//
        ///////////////////////////////////////////


        //////////////////////////////////////////////
        //tama on apufunktion Levensthein distanceen//
        //////////////////////////////////////////////
        private int minimum(int a, int b, int c)
        {
            return Math.Min(Math.Min(a, b), c);
        }

        ////////////////////////////////////////////////
        //tassa on itse Levenshtein-distance-algoritmi//
        ////////////////////////////////////////////////
        public int computeLevenshteinDistance(string str1, string str2)
        {

            str1 = str1.ToUpper().Trim();
            str2 = str2.ToUpper().Trim();

            int[,] distance = new int[str1.Length + 1, str2.Length + 1];

            for (int i = 0; i <= str1.Length; i++)
                distance[i, 0] = i;
            for (int j = 1; j <= str2.Length; j++)
                distance[0, j] = j;

            for (int i = 1; i <= str1.Length; i++)
                for (int j = 1; j <= str2.Length; j++)
                    distance[i, j] = minimum(
                            distance[i - 1, j] + 1,
                            distance[i, j - 1] + 1,
                            distance[i - 1, j - 1] + ((str1[i - 1] == str2[j - 1]) ? 0 : 1));

            return distance[str1.Length, str2.Length];
        }

    }
}
