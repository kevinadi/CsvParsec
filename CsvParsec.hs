import System.Environment
import System.IO
import Data.Either (either)
import Data.Ord
import Data.List
import Data.Char
import Data.Time
import Data.Maybe
import Control.Applicative ((<$>),(*>),(<*),(<*>),pure)
import Control.Monad.State.Strict
import Control.Monad.Reader
import Text.Parsec
import Text.Parsec.String  (Parser)


--csv line parser
------------------------------------------------------------------
parsec_unquotedcell :: (Char,Char) -> Parser String
parsec_unquotedcell (cs,ds) = many (noneOf [cs])

parsec_quotedcell :: (Char,Char) -> Parser String
parsec_quotedcell (cs,ds) = char ds *> many (noneOf [ds] <|> try (string [ds,ds] >> return ds)) <* char ds

parsec_line :: (Char,Char) -> Parser [String]
parsec_line (cs,ds) = sepBy (parsec_quotedcell (cs,ds) <|> parsec_unquotedcell (cs,ds)) (char cs)

parseCsv :: (Char,Char) -> String -> [String]
parseCsv (cs,ds) = either (const []) id . parse (parsec_line (cs,ds)) ""

alternateParser :: (Char,Char) -> Int -> String -> Maybe [String]
alternateParser (cs,ds) colcount line =
    if length cols == colcount && head line == ds && last line == ds
        then Just cols
        else Nothing
    where cols = splitStr [ds,cs,ds] . tail . init $ line
------------------------------------------------------------------


--helpers
------------------------------------------------------------------
splitStr :: Eq a => [a] -> [a] -> [[a]]
splitStr sub str = split' sub str [] []
    where
    split' _   []  subacc acc = reverse (reverse subacc : acc)
    split' sub str subacc acc
        | sub `isPrefixOf` str = split' sub (drop (length sub) str) [] (reverse subacc:acc)
        | otherwise            = split' sub (tail str) (head str:subacc) acc


longestCol :: [String] -> Int
longestCol l = if length l > 0
    then length . maximumBy (comparing length) $ l
    else 0


filterLine :: String -> String
filterLine = filter (/='\r') . filter (/='\n')
------------------------------------------------------------------


--outputs
------------------------------------------------------------------
outputCsv :: (Char,Char) -> [String] -> String
outputCsv (cs,ds) = intercalate [cs] . map (\x -> [ds] ++ (outputCol ds x) ++ [ds])
    where outputCol ds = foldr (\a b -> if a == ds then ds:ds:b else a:b) []


outputCreateTable :: String -> [String] -> String
outputCreateTable tbl x  = dropTable ++ "CREATE TABLE "++tbl++" (\n" ++ (cols x) ++ ")"
    where cols       = intercalate ",\n" . map (\x -> "[" ++ (cleanup x) ++ "] NVARCHAR(" ++ (idcol x) ++ ") NULL")
          idcol c    = if isInfixOf "UniqueID" c then "255" else "MAX"
          cleanup    = replaceSpc . printables . trim
          replaceSpc = foldr (\a b -> if a==' ' then '_':b else a:b) []
          printables = filter (\x -> isAlphaNum x || isSeparator x)
          trim       = unwords . words
          dropTable  = "IF OBJECT_ID('"++tbl++"','U') IS NOT NULL DROP TABLE "++tbl++"\n"


outputSQL :: String -> [String] -> String
outputSQL tbl x = "INSERT INTO "++tbl++" VALUES (" ++ (cols x) ++ ")"
    where cols = intercalate "," . map (\x -> "\'" ++ (quote x) ++ "\'")
          quote = foldr (\a b -> if a == '\'' then '\'':'\'':b else a:b) []


printState :: ProcState -> String
printState state = "--Processed line: " ++ show (totallines state) ++ "\n"
                   ++ "--Longest column size: " ++  show (longcolsize state)
                   ++ " on line " ++ show (longcolline state) ++ "\n"
                   ++ "--Last line: " ++ (if null (prevline state) then "Empty" else (prevline state))
------------------------------------------------------------------


--IO stdin/file
------------------------------------------------------------------
getInputFileContent :: [String] -> IO String
getInputFileContent args =
    let getContents f = hSetNewlineMode f universalNewlineMode >> hGetContents f in
    if "-d" `elem` args
        then getContents stdin
        else case filter (\x -> head x /= '-') args of
            []    -> getContents stdin
            (f:_) -> do
                fileHandle <- openFile f ReadMode
                hSetBuffering fileHandle . BlockBuffering . Just $ 1024*10
                hSetEncoding fileHandle utf8
                getContents fileHandle
------------------------------------------------------------------


--initial environment checks
------------------------------------------------------------------
checkCsv :: String -> (Char,Char)
checkCsv str = fst . last $ sortBy (comparing snd) (procLength)
    where testenv = [ Env { csv = (',','"'), out = (',','"'), maxcolen = 0,
                        sql = False, sqltable = "", colcount = 0 }
                    , Env { csv = ('|','~'), out = ('|','~'), maxcolen = 0,
                        sql = False, sqltable = "", colcount = 0 }
                    , Env { csv = ('|','"'), out = ('|','~'), maxcolen = 0,
                        sql = False, sqltable = "", colcount = 0 }
                    ]
          procLength = map (\envs ->
                            (csv envs,
                            (length $ parseCsv (csv envs) str) +
                            (length $ filter (==snd (csv envs)) str) ))
                            testenv
------------------------------------------------------------------


-- main loop
----------------------------------------------------------------------------
doProcess :: [String] -> StateT ProcState (ReaderT Env IO) ()

doProcess [] = do
    state <- get
    if (not.null) (prevline state)
        then parseLine (prevline state)
        else return ()

doProcess ([]:rest) = do
    state <- get
    put state { totallines = (totallines state)+1 }
    liftIO . hPutStrLn stderr $ "--Empty line on " ++ show (totallines state)
    doProcess rest

doProcess (line:rest) = do
    state <- get
    put state { totallines = (totallines state)+1 }
    parseLine $ (prevline state) ++ line
    doProcess rest


parseLine :: String -> StateT ProcState (ReaderT Env IO) ()
parseLine line = do
    env <- ask
    state <- get
    case parse (parsec_line (csv env)) "" line of
        Right cols -> do
            if length cols == colcount env
                then do
                    checkLongColumn cols
                    if (all (\x -> length x <= (maxcolen env)) cols)
                        then processCleanLine cols
                        else liftIO . hPutStrLn stdout $ outputSQL (sqltable env) cols
                else processDirtyLine line
        Left _ -> put state { prevline = filterLine line }

checkLongColumn :: [String] -> StateT ProcState (ReaderT Env IO) ()
checkLongColumn cols = do
    state <- get
    let longcol = longestCol cols
    if longcol > (longcolsize state)
        then put state { longcolline = (totallines state), longcolsize = longcol }
        else return ()

processCleanLine :: [String] -> StateT ProcState (ReaderT Env IO) ()
processCleanLine cols = do
    modify (\state -> state { prevline = "" })
    env <- ask
    if (sql env)
        then liftIO . hPutStrLn stdout $ outputSQL (sqltable env) cols
        else liftIO . hPutStrLn stdout $ outputCsv (out env) cols

processDirtyLine :: String -> StateT ProcState (ReaderT Env IO) ()
processDirtyLine line = do
    state <- get
    env <- ask
    case alternateParser (csv env) (colcount env) line of
        Just cols -> do
            liftIO . hPutStrLn stderr $ outputSQL (sqltable env) cols
            put state { prevline = "" }
        Nothing -> put state { prevline = filterLine line }
----------------------------------------------------------------------------


-- reader monad environment
----------------------------------------------------------------------------
data Env = Env
    { csv      :: (Char,Char)
    , out      :: (Char,Char)
    , maxcolen :: Int
    , sql      :: Bool
    , sqltable :: String
    , colcount :: Int
    } deriving Show
----------------------------------------------------------------------------



-- state monad environment
----------------------------------------------------------------------------
data ProcState = ProcState
    { totallines  :: Int
    , longcolline :: Int
    , longcolsize :: Int
    , prevline    :: String
    } deriving Show
----------------------------------------------------------------------------



-- table name command line argument
----------------------------------------------------------------------------
argProcess :: String -> [String] -> String
argProcess cmd args = let
    wordsWhen p s =  case dropWhile p s of
        "" -> []
        s' -> w : wordsWhen p s''
            where (w, s'') = break p s'
    table_name = filter (isPrefixOf cmd) args
    file_arg   = listToMaybe . filter (\x -> head x /= '-') $ args
    file_name  = map toUpper . head . wordsWhen (=='.') . fromMaybe "--X--" $ file_arg
    in case table_name of
        [] -> file_name
        i  -> drop 2 $ head i
----------------------------------------------------------------------------



-- MAIN
----------------------------------------------------------------------------
main :: IO ()
main = do
    hSetBuffering stdout . BlockBuffering . Just $ 1024*10
    hSetBuffering stderr . BlockBuffering . Just $ 1024*10
    hSetEncoding stdin  utf8_bom
    hSetEncoding stdout utf8
    hSetEncoding stderr utf8

    args <- getArgs

    if length args == 0
        then putStrLn "CsvParsec [-d|input_file] -t[table_name] [-sql] 1>[cleaned_output] 2>[errors]"
        else do

            start <- getCurrentTime

            contents <- getInputFileContent args
            let contentLines = lines contents

            ------ reader monad
            let initialenv = Env {
                csv      = checkCsv (head contentLines),
                out      = ('|','~'),
                maxcolen = 3000,
                sql      = elem "-sql" args,
                sqltable = argProcess "-t" args,
                colcount = 0
            }

            let headercols = parseCsv (csv initialenv) (head contentLines)
            let env = initialenv { colcount = (length headercols) }

            hPutStrLn stderr $ outputCreateTable (sqltable env) headercols
            hPutStrLn stderr "-------------------------------"

            ------- main process
            let state = ProcState {
                totallines  = 1, -- line count includes header line
                longcolline = 0,
                longcolsize = 0,
                prevline    = ""
            }
            finalState <- flip runReaderT env . flip execStateT state . doProcess $ tail contentLines

            hPutStrLn stderr "-------------------------------"
            hPutStrLn stderr $ printState finalState

            stop <- getCurrentTime

            hPutStrLn stderr $ "--Columns: "        ++ show (colcount env)
            hPutStrLn stderr $ "--Env setting: "    ++ show env
            hPutStrLn stderr $ "--Execution time: " ++ show (diffUTCTime stop start)
----------------------------------------------------------------------------
